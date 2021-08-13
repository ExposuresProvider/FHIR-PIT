package datatrans.step

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession, DataFrame}
import io.circe._
import io.circe.generic.semiauto._
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time._
import scopt._
import org.apache.spark.sql.functions._
import datatrans.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._
import datatrans.Mapper

case class PreprocCSVTableConfig(
  input_dir : String = null,
  input_dir_patient : String = null,
  output_dir : String = null,
  deidentify : Seq[String] = Seq(),
  study_period_start: DateTime = null,
  offset_hours : Int = 0,
  feature_map : String = null
)

object PreprocCSVTable extends StepImpl {
  
  type ConfigType = PreprocCSVTableConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config:PreprocCSVTableConfig) : Unit = {
    import spark.implicits._

    def make_env_schema(names: Seq[String]) = StructType(
      StructField("start_date", DateType, true) +:
        (for(
          generator <- Seq(
            (i:String) => i,
            (i:String) => i + "_avg",
            (i:String) => i + "_max"
          );
          name <- names
        ) yield StructField(generator(name), DoubleType, false)).toSeq
    )

    val env_schema = make_env_schema(Seq(
      "o3_avg",
      "pm25_avg",
      "o3_max",
      "pm25_max"
    ))

    val env2_schema = make_env_schema(Seq(
      "ozone_daily_8hour_maximum",
      "pm25_daily_average",
      "CO_ppbv",
      "NO_ppbv",
      "NO2_ppbv",
      "NOX_ppbv",
      "SO2_ppbv",
      "ALD2_ppbv",
      "FORM_ppbv",
      "BENZ_ppbv"
    ))

    def expandDataFrame(df: DataFrame, schema : StructType) =
      schema.fields.toSeq.foldLeft(df)((df0: DataFrame, field: StructField) => {
        val f = field.name
        if (df.columns.contains(f)) df0 else df0.withColumn(f, lit(null).cast(DoubleType))
      })

    def join_env(hc: Configuration, pat_df: DataFrame, schema: StructType, menv_file: Option[String], cols : String) : DataFrame = {
      menv_file match {
        case Some(env_file) =>
          if(fileExists(hc, env_file)) {
            val env_df = readCSV2(spark, env_file, schema, _ => DoubleType)

            pat_df.join(env_df, Seq(cols), "left")
          } else {
            println(f"file not exists $env_file")
            pat_df // expandDataFrame(pat_df, env_schema)
          }
        case None =>
          println(f"no file name provided")
          pat_df // expandDataFrame(pat_df, env_schema)
      }
    }

    time {
      val hc = spark.sparkContext.hadoopConfiguration
      val mapper = new Mapper(hc, config.feature_map)

      val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)

      // df match {
      //   case Some(df) =>
      //     spark.sparkContext.broadcast(df)
      //   case _ =>
      // }

      val input_dir_visit = config.input_dir
      val input_dir_patient = config.input_dir_patient

      println("combining output")
      val output_file_visit_combined = config.output_dir + "/visit_combined"
      if(fileExists(hc, output_file_visit_combined)) {
        println(output_file_visit_combined +  " exists")
      } else {
        combineCSVs(hc, input_dir_visit, output_file_visit_combined)
      }

//      val output_file_patient_combined = config.output_dir + "/patient_combined"
//      if(fileExists(hc, output_file_patient_combined)) {
//        println(output_file_patient_combined +  " exists")
//      } else {
//        combineCSVs(hc, input_dir_patient, output_file_patient_combined)
//      }

      println("aggregation")

      val output_all_visit = config.output_dir + "/all_visit"
      val output_all_patient = config.output_dir + "/all_patient"
      if(fileExists(hc, output_all_patient)) {
        println(output_all_patient + " exists")
      } else {
        val ageYear = udf((birthDate : String) => {
          val birth_date_joda = DateTime.parse(birthDate, ISODateTimeFormat.dateParser())
          val study_start_joda = config.study_period_start
          Years.yearsBetween(birth_date_joda, study_start_joda).getYears
        })

        val ageDiff = udf((birthDate : String, start_date: String) => {
          val birth_date_joda = DateTime.parse(birthDate, ISODateTimeFormat.dateParser())
          val study_start_joda = DateTime.parse(start_date, ISODateTimeFormat.dateParser())
          Years.yearsBetween(birth_date_joda, study_start_joda).getYears
        })
        class TotalTypeVisits(vtype : Seq[String], nvtype:Seq[String] = Seq()) extends UserDefinedAggregateFunction {
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
            if(respiratoryDx && visitType != null && vtype.exists(visitType.contains(_)) && !nvtype.exists(visitType.contains(_))) {
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

        val binary = mapper.conds | mapper.meds | mapper.procs | mapper.labs
       val values_first = mapper.labs.flatMap(x => Seq(f"${x}_value_first", f"${x}_flag_first"))
       val values_last = mapper.labs.flatMap(x => Seq(f"${x}_value_last", f"${x}_flag_last"))

        var df_visit_combined = spark.read.format("csv").option("header", value = true).load(output_file_visit_combined)
//        var df_patient_combined = spark.read.format("csv").option("header", value = true).load(output_file_patient_combined)
        (binary -- df_visit_combined.columns.toSet).foreach(col => {
          df_visit_combined = df_visit_combined.withColumn(col, lit(0))
        })

        val sex2gen = udf((s : String) => s match {
          case "Male" => s
          case "Female" => s
          case _ => null
        })
        
        df_visit_combined = df_visit_combined
          // .withColumn("year", year($"start_date"))
          .withColumn("Sex2", sex2gen($"Sex"))

        df_visit_combined = expandDataFrame(df_visit_combined, env_schema)
        df_visit_combined = expandDataFrame(df_visit_combined, env2_schema)

        val procObesityBMI = udf((x : Double) => if(x >= 30) 1 else 0)

        var df_all_visit = df_visit_combined
          .withColumn("AgeVisit", ageDiff($"birth_date", $"start_date"))

        val binarySeq = binary.toSeq
        val binaryVisitSeq = binarySeq.map(v => v + "Visit")

        (binarySeq zip binaryVisitSeq).foreach{
          case (b, bv) => {
            df_all_visit = df_all_visit.withColumnRenamed(b, bv)
          }
        }

        val df_all_visit_column_set = df_all_visit.columns.toSet
        for ((feature1, feature2) <- Mapper.visitEnvFeatureMapping)
          if(df_all_visit_column_set.contains(feature1))
            df_all_visit = df_all_visit.withColumnRenamed(feature1, feature2)
          else
            df_all_visit = df_all_visit.withColumn(feature2, lit(""))

        for (feature <- Mapper.mappedEnvOutputColumns)
          df_all_visit = df_all_visit.drop(feature)

        df_all_visit = df_all_visit
          .withColumn("ObesityBMIVisit", procObesityBMI($"ObesityBMIVisit"))

        // df_all_visit = df_all_visit.na.fill(0, "ObesityBMIVisit" +: binaryVisitSeq) // spark has a bug in na such that column with dot doesn't work
        ("ObesityBMIVisit" +: binaryVisitSeq).foreach(colname => {
          df_all_visit = df_all_visit.withColumn(colname, when(df_all_visit.col(colname).isNull, 0).otherwise(df_all_visit.col(colname)))
        })

        val deidentify = df_all_visit.columns.intersect(config.deidentify)
        df_all_visit = df_all_visit
          .drop(deidentify : _*)
        df_all_visit = df_all_visit.filter($"AgeVisit" < 90)

        writeDataframe(hc, output_all_visit, df_all_visit)

        val emerTypes = Seq("AMB","EMER")
        val inpatientTypes = Seq("IMP")

       val dfc = df_visit_combined.columns
       val dfcs = dfc.toSet
        val patient_aggs =
          (
            for (
              (feature1, feature2) <- Mapper.patientEnvFeatureMapping
            ) yield first(df_visit_combined.col(feature1)).alias(feature2)
          ) ++ Seq(
            max(df_visit_combined.col("ObesityBMIVisit")).alias("ObesityBMI"),
            new TotalTypeVisits(emerTypes)($"VisitType", $"RespiratoryDx").alias("TotalEDVisits"),
            new TotalTypeVisits(inpatientTypes)($"VisitType", $"RespiratoryDx").alias("TotalInpatientVisits"),
            (new TotalTypeVisits(emerTypes)($"VisitType", $"RespiratoryDx") + new TotalTypeVisits(inpatientTypes, emerTypes)($"VisitType", $"RespiratoryDx")).alias("TotalEDInpatientVisits")
          ) ++ (("Sex2" +: Mapper.demograph) ++ mapper.nearest_road_map_map.values.flatMap(x => x.distance_feature_name +: x.attributes_to_features_map.values.map(_.feature_name).toSeq) ++ mapper.nearest_point_map_map.values.flatMap(x => x.distance_feature_name +: x.attributes_to_features_map.values.map(_.feature_name).toSeq) ++ mapper.geoid_map_map.values.flatMap(_.columns.values)).map(
            v => first(df_visit_combined.col(v)).alias(v)
          ) ++ (binary intersect dfcs).map(
            v => sum(df_visit_combined.col(v)).cast(IntegerType).alias(v)
          ) ++ (values_first intersect dfcs).map(
           v => first(df_visit_combined.col(v)).alias(v)
          ) ++ (values_last intersect dfcs).map(
            v => last(df_visit_combined.col(v)).alias(v)
          )

        val df_all15 = if (mapper.visit.isEmpty)
         df_visit_combined.withColumn("RespiratoryDx", lit(true))
        else
          df_visit_combined.withColumn("RespiratoryDx", (mapper.visit intersect dfc.toSeq).map(a => df_visit_combined.col(a).cast(IntegerType) === 1).reduce(_ || _))

        val df_all2 = df_all15.groupBy("patient_num", "year").agg(patient_aggs.head, patient_aggs.tail:_*)

        val df_active_in_year = df_all_visit.select("patient_num", "year").distinct().withColumn("Active_In_Year", lit(1))

        var df_all_patient = df_all2
          .withColumn("AgeStudyStart", ageYear($"birth_date", $"year"))
          .withColumn("ObesityBMI", procObesityBMI($"ObesityBMI"))
          .join(broadcast(df_active_in_year), Seq("patient_num", "year"), "left")
        // .na.fill(0, "Active_In_Year" +: "ObesityBMI" +: binary.toSeq) spark has a bug

        ("Active_In_Year" +: "ObesityBMI" +: binary.toSeq).foreach(colname => {
          df_all_patient = df_all_patient.withColumn(colname, when(df_all_patient.col(colname).isNull, 0).otherwise(df_all_patient.col(colname)))
        })

        val deidentify2 = df_all_patient.columns.intersect(config.deidentify)
        df_all_patient = df_all_patient.drop(deidentify2 : _*)
        df_all_patient = df_all_patient.filter($"AgeStudyStart" < 90)
        writeDataframe(hc, output_all_patient, df_all_patient)
      }

    }

  }

}
