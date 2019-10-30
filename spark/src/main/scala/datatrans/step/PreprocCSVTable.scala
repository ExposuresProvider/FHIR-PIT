package datatrans

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time._
import scopt._
import org.apache.spark.sql.functions._
import net.jcazevedo.moultingyaml._
import datatrans.Utils._
import datatrans.ConditionMapper._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocCSVTableConfig(
  patient_file : String = "",
  environment_file : String = "",
  input_files : Seq[String] = Seq(),
  output_file : String = "",
  start_date : DateTime = new DateTime(0),
  end_date : DateTime = new DateTime(0),
  deidentify : Seq[String] = Seq()
) extends StepConfig {
  val configConfig = PreprocCSVTable
}

object CSVTableYamlProtocol extends SharedYamlProtocol {
  implicit val csvTableYamlFormat = yamlFormat7(PreprocCSVTableConfig)
}

object PreprocCSVTable extends StepConfigConfig {
  
  type ConfigType = PreprocCSVTableConfig

  val yamlFormat = CSVTableYamlProtocol.csvTableYamlFormat

  val configType = "CSVTable"

  def step(spark: SparkSession, config:PreprocCSVTableConfig) : Unit = {
    import spark.implicits._

    val env_schema = StructType(
      StructField("start_date", DateType, true) ::
        (for(
          generator <- Seq((i:String) => i, (i:String) => i + "_avg", (i:String) => i + "_max", (i:String) => i + "_min", (i:String) => i + "_stddev");
          name <- Seq("o3_avg", "pm25_avg", "o3_max", "pm25_max", "ozone_daily_8hour_maximum", "pm25_daily_average")
        ) yield StructField(generator(name), DoubleType, false)).toList)

    println(env_schema)

    time {
      val hc = spark.sparkContext.hadoopConfiguration
      val start_date_joda = config.start_date
      val end_date_joda = config.end_date

      val dfs = config.input_files.map(input_file => {
        spark.read.format("csv").option("header", value = true).load(input_file)
      })

      val df = dfs.reduce(_.join(_, "patient_num"))

      spark.sparkContext.broadcast(df)

      val plusOneDayDate = udf((x : String) =>
        DateTime.parse(x, ISODateTimeFormat.dateParser()).plusDays(1).toString("yyyy-MM-dd")
      )

      val per_pat_output_dir = config.output_file + "/per_patient"
      withCounter(count =>
        new HDFSCollection(hc, new Path(config.patient_file)).foreach(f => {
          val p = f.getName()
          println("processing patient " + count.incrementAndGet() + " " + p)
          val output_file = per_pat_output_dir + "/" + p
          if(fileExists(hc, output_file)) {
            println("file exists " + output_file)
          } else {
            val pat_df = spark.read.format("csv").option("header", value = true).load(f.toString())

            if(!pat_df.head(1).isEmpty) {
              val env_file = config.environment_file + "/" + p
              if(fileExists(hc, env_file)) {
                val env_df = spark.read.format("csv").option("header", value = true).schema(env_schema).load(env_file)
                val env_df2 = env_df.withColumn("next_date", plusOneDayDate(env_df.col("start_date"))).drop("start_date").withColumnRenamed("next_date", "start_date")

                val patenv_df0 = pat_df.join(env_df2, Seq("start_date"), "left")
                val patenv_df = patenv_df0.join(df, Seq("patient_num"), "left")
                writeDataframe(hc, output_file, patenv_df)
              } else {
                println("warning: no record is contructed because env file does not exist for " + p)
              }
            }
          }

        })

      )

      println("combining output")
      val output_file_all = config.output_file + "/all"
      if(fileExists(hc, output_file_all)) {
        println(output_file_all +  " exists")
      } else {
        combineCSVs(hc, per_pat_output_dir, output_file_all)
      }

      println("aggregation")

      val output_all_visit = config.output_file + "/all_visit"
      val output_all_patient = config.output_file + "/all_patient"
      if(fileExists(hc, output_all_patient)) {
        println(output_all_patient + " exists")
      } else {
        val year = udf((date : String) => DateTime.parse(date, ISODateTimeFormat.dateParser()).year.get)
        val ageYear = udf((birthDate : String, year: Int) => {
          val birth_date_joda = DateTime.parse(birthDate, ISODateTimeFormat.dateParser())
          val study_start_joda = new DateTime(year, 1, 1, 0, 0, 0)
          Years.yearsBetween(birth_date_joda, study_start_joda).getYears
        })

        val ageDiff = udf((birthDate : String, start_date: String) => {
          val birth_date_joda = DateTime.parse(birthDate, ISODateTimeFormat.dateParser())
          val study_start_joda = DateTime.parse(start_date, ISODateTimeFormat.dateParser())
          Years.yearsBetween(birth_date_joda, study_start_joda).getYears
        })
        class TotalTypeVisits(vtype : String) extends UserDefinedAggregateFunction {
          // This is the input fields for your aggregate function.
          override def inputSchema: org.apache.spark.sql.types.StructType =
            StructType(StructField("VisitType", StringType) :: StructField("RespiratoryDx", BooleanType) :: Nil)

          // This is the internal fields you keep for computing your aggregate.
          override def bufferSchema: StructType = StructType(
            StructField("count", IntegerType) :: Nil
          )

          // This is the output type of your aggregatation function.
          override def dataType: DataType = IntegerType


          override def deterministic: Boolean = true

          // This is the initial value for your buffer schema.
          override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0
          }

          // This is how to update your buffer schema given an input.
          override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            val visitType = input.getAs[String](0)
            val respiratoryDx = input.getAs[Boolean](1)
            if(respiratoryDx && visitType.contains(vtype)) {
              buffer(0) = buffer.getAs[Int](0) + 1
            }
          }

          // This is how to merge two objects with the bufferSchema type.
          override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
          }

          // This is where you output the final value, given the final value of your bufferSchema.
          override def evaluate(buffer: Row): Any = {
            buffer.getInt(0)
          }
        }

        val visit = dx ++ meds

        var df_all = spark.read.format("csv").option("header", value = true).load(output_file_all)
        visit.diff(df_all.columns).foreach(col => {
          df_all = df_all.withColumn(col, lit(0))
        })

        val sex2gen = udf((s : String) => s match {
          case "Male" => s
          case "Female" => s
          case _ => null
        })
        
        df_all = df_all
          .withColumn("year", year($"start_date"))
          .withColumn("Sex2", sex2gen($"Sex"))

        val procObesityBMI = udf((x : Double) => if(x >= 30) 1 else 0)

        var df_all_visit = df_all
          .withColumn("AgeVisit", ageDiff($"birth_date", $"start_date"))

        visit.foreach(v => {
          df_all_visit = df_all_visit.withColumnRenamed(v,v + "Visit")
        })

        for ((feature1, feature2) <- Seq(("pm25", "PM2.5"), ("o3", "Ozone")); (stat1, stat2) <- Seq(("avg", "Avg"), ("max", "Max"))) {
          df_all_visit = df_all_visit.withColumnRenamed(feature1 + "_" + stat1, stat2 + "24h" + feature2 + "Exposure")
          for (stat_b <- Seq("avg", "max", "min", "stddev"))
            df_all_visit = df_all_visit.drop(feature1 + "_" + stat1 + "_" + stat_b)
        }


        for ((feature1, feature2) <- Seq(("pm25_daily_average", "Avg24hPM2.5"), ("ozone_daily_8hour_maximum", "Max24hOzone"))) {
          df_all_visit = df_all_visit.withColumnRenamed(feature1, feature2 + "Exposure_2")
          for (stat_b <- Seq("avg", "max", "min", "stddev"))
            df_all_visit = df_all_visit.drop(feature1 + "_" + stat_b)
        }

        df_all_visit = df_all_visit
          .withColumnRenamed("ObesityBMIVisit", "ObesityBMIVisit0")
          .withColumn("ObesityBMIVisit", procObesityBMI($"ObesityBMIVisit0"))
          .drop("ObesityBMIVisit0")

        val deidentify = df_all_visit.columns.intersect(config.deidentify)
        df_all_visit = df_all_visit
          .drop(deidentify : _*)
        df_all_visit = df_all_visit.filter($"AgeVisit" < 90)

        writeDataframe(hc, output_all_visit, df_all_visit)

        val patient_aggs =
          (for ((feature1, feature2) <- Seq(("pm25", "PM2.5"), ("o3", "Ozone")); (stat1, stat2) <- Seq(("avg", "Avg"), ("max", "Max")); (stat1_b, stat2_b) <- Seq(("avg", "Avg"), ("max", "Max")))
          yield first(df_all.col(feature1 + "_" + stat1 + "_" + stat1_b)).alias(stat2 + "Daily" + feature2 + "Exposure_Study" + stat2_b)) ++
        (for ((feature1, feature2) <- Seq(("pm25", "PM2.5"), ("o3", "Ozone")); (stat1, stat2) <- Seq(("avg", "Avg"), ("max", "Max")))
        yield first(df_all.col(feature1 + "_" + stat1 + "_" + stat1)).alias(stat2 + "Daily" + feature2 + "Exposure")) ++
        (for ((feature1, feature2) <- Seq(("pm25_daily_average", "AvgDailyPM2.5"), ("ozone_daily_8hour_maximum", "MaxDailyOzone")))
        yield first(df_all.col(feature1 + "_avg")).alias(feature2 + "Exposure_2")
        ) ++
        Seq(
          max(df_all.col("ObesityBMIVisit")).alias("ObesityBMI"),
          new TotalTypeVisits("EMER")($"VisitType", $"RespiratoryDx").alias("TotalEDVisits"),
          new TotalTypeVisits("IMP")($"VisitType", $"RespiratoryDx").alias("TotalInpatientVisits"),
          (new TotalTypeVisits("EMER")($"VisitType", $"RespiratoryDx") + new TotalTypeVisits("IMP")($"VisitType", $"RespiratoryDx")).alias("TotalEDInpatientVisits")) ++
        demograph.map(v => first(df_all.col(v)).alias(v)) ++
        acs.map(v => first(df_all.col(v)).alias(v)) ++
        visit.map(v => max(df_all.col(v)).alias(v))

        val df_all2 = df_all
          .withColumn("RespiratoryDx", $"AsthmaDx" === "1" || $"CroupDx" === "1" || $"ReactiveAirwayDx" === "1" || $"CoughDx" === "1" || $"PneumoniaDx" === "1")
          .groupBy("patient_num", "year").agg(patient_aggs.head, patient_aggs.tail:_*)
        var df_all_patient = df_all2
          .withColumn("AgeStudyStart", ageYear($"birth_date", $"year"))
          .withColumnRenamed("ObesityBMI", "ObesityBMI0")
          .withColumn("ObesityBMI", procObesityBMI($"ObesityBMI0"))
          .drop("ObesityBMI0")
        val deidentify2 = df_all_patient.columns.intersect(config.deidentify)
        df_all_patient = df_all_patient.drop(deidentify2 : _*)
        df_all_patient = df_all_patient.filter($"AgeStudyStart" < 90)
        writeDataframe(hc, output_all_patient, df_all_patient)
      }

    }

  }

}
