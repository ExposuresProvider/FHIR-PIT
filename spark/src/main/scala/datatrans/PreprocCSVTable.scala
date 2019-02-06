package datatrans

import java.io._

import datatrans.Utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.types.{ StringType, StructField, StructType, IntegerType, DataType, DateType, DoubleType }
import org.apache.spark.sql.{ SparkSession, Column, Row }
import org.joda.time.Years
import org.joda.time.format.ISODateTimeFormat
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scopt._
import datatrans._
import org.apache.spark.sql.functions._

case class PreprocCSVTableConfig(
  patient_file : String = "",
  environment_file : String = "",
  input_files : Seq[String] = Seq(),
  output_file : String = "",
  start_date : DateTime = new DateTime(0),
  end_date : DateTime = new DateTime(0),
  deidentify : Seq[String] = Seq()
)

object PreprocCSVTable {
  
  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocCSVTableConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_directory").required.action((x,c) => c.copy(patient_file = x))
      opt[String]("environment_directory").required.action((x,c) => c.copy(environment_file = x))
      opt[String]("input_files").required.action((x,c) => c.copy(input_files = x.split(",").filter(_.nonEmpty)))
      opt[String]("output_directory").required.action((x,c) => c.copy(output_file = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("end_date").action((x,c) => c.copy(end_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("deidentify").action((x,c) => c.copy(deidentify = x.split(",").filter(_.nonEmpty)))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val env_schema = StructType(
      StructField("start_date", DateType, true) ::
        (for(
          g <- Seq((i:String) => i, (i:String) => i + "_avg", (i:String) => i + "_max");
          m <- Seq("o3_avg", "pm25_avg", "o3_max", "pm25_max", "ozone_daily_8hour_maximum", "pm25_daily_average")
        ) yield StructField(g(m), DoubleType, false)).toList)

    parser.parse(args, PreprocCSVTableConfig()) match {
      case Some(config) =>

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

                    val patenv_df0 = pat_df.join(env_df2, "start_date")
                    val patenv_df = patenv_df0.join(df, "patient_num")
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
            class TotalEDInpatientVisits extends UserDefinedAggregateFunction {
              // This is the input fields for your aggregate function.
              override def inputSchema: org.apache.spark.sql.types.StructType =
                StructType(StructField("VisitType", StringType) :: Nil)

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
                if(visitType == "IMP" || visitType == "EMER") {
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

            df_all = df_all
              .withColumn("year", year($"start_date"))

            val procObesityBMI = udf((x : Double) => if(x >= 30) 1 else 0)

            var df_all_visit = df_all
              .withColumn("AgeVisit", ageDiff($"birth_date", $"start_date"))

            visit.foreach(v => {
              df_all_visit = df_all_visit.withColumnRenamed(v,v + "Visit")
            })

            df_all_visit = df_all_visit
              .withColumnRenamed("ObesityBMIVisit", "ObesityBMIVisit0")
              .withColumn("ObesityBMIVisit", procObesityBMI($"ObesityBMIVisit0"))
              .drop("ObesityBMIVisit0")

            val deidentify = df_all_visit.columns.intersect(config.deidentify)
            df_all_visit = df_all_visit
              .drop(deidentify : _*)

            writeDataframe(hc, output_all_visit, df_all_visit)

            val patient_aggs = Seq(
              first(df_all.col("pm25_avg_avg")).alias("AvgDailyPM2.5Exposure"),
              first(df_all.col("pm25_avg_max")).alias("AvgDailyPM2.5Exposure_max"),
              first(df_all.col("pm25_max_avg")).alias("MaxDailyPM2.5Exposure"),
              first(df_all.col("pm25_max_max")).alias("MaxDailyPM2.5Exposure_max"),
              first(df_all.col("o3_avg_avg")).alias("AvgDailyOzoneExposure"),
              first(df_all.col("o3_avg_max")).alias("AvgDailyOzoneExposure_max"),
              first(df_all.col("o3_max_avg")).alias("MaxDailyOzoneExposure"),
              first(df_all.col("o3_max_max")).alias("MaxDailyOzoneExposure_max"),
              first(df_all.col("pm25_daily_average_avg")).alias("AvgDailyPM2.5Exposure_2"),
              first(df_all.col("ozone_daily_8hour_maximum_avg")).alias("MaxDailyOzoneExposure_2"),
              max(df_all.col("ObesityBMIVisit")).alias("ObesityBMI"),
              new TotalEDInpatientVisits()(df_all.col("VisitType")).alias("TotalEDInpatientVisits")) ++ demograph.map(v => first(df_all.col(v)).alias(v)) ++ acs.map(v => first(df_all.col(v)).alias(v)) ++ visit.map(v => max(df_all.col(v)).alias(v))

            val df_all2 = df_all
              .groupBy("patient_num", "year").agg(patient_aggs.head, patient_aggs.tail:_*)
            var df_all_patient = df_all2
              .withColumn("AgeStudyStart", ageYear($"birth_date", $"year"))
              .withColumnRenamed("ObesityBMI", "ObesityBMI0")
              .withColumn("ObesityBMI", procObesityBMI($"ObesityBMI0"))
              .drop("ObesityBMI0")
            val deidentify2 = df_all_patient.columns.intersect(config.deidentify)
            df_all_patient = df_all_patient.drop(deidentify2 : _*)
            writeDataframe(hc, output_all_patient, df_all_patient)
          }


        }
      case None =>
    }


    spark.stop()
  }

}
