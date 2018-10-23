package datatrans

import java.io._

import datatrans.Utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.types.{ StringType, StructField, StructType, IntegerType, DataType }
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
                  val env_df = spark.read.format("csv").option("header", value = true).load(config.environment_file + "/" + p)
                  val env_df2 = env_df.withColumn("next_date", plusOneDayDate(env_df.col("start_date"))).drop("start_date").withColumnRenamed("next_date", "start_date")
                  
                  val patenv_df = pat_df.join(env_df2, "start_date").join(df, "patient_num")
                  writeDataframe(hc, output_file, patenv_df)
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
              Years.yearsBetween(study_start_joda, birth_date_joda).getYears
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

            var df_all = spark.read.format("csv").option("header", value = true).load(output_file_all)
            df_all = df_all.withColumn("year", year(df_all.col("start_date")))
            df_all = df_all.withColumn("AgeStudyStart", ageYear(df_all.col("birth_date"), df_all.col("year")))
            Seq(
              "AsthmaDx",
              "CroupDx",
              "ReactiveAirwayDx",
              "CoughDx",
              "PneumoniaDx",
              "ObesityDx",
              "ObesityBMI",
              "Prednisone",
              "Fluticasone",
              "Mometasone",
              "Budesonide",
              "Beclomethasone",
              "Ciclesonide",
              "Flunisolide",
              "Albuterol",
              "Metaproterenol",
              "Diphenhydramine",
              "Fexofenadine",
              "Cetirizine",
              "Ipratropium",
              "Salmeterol",
              "Arformoterol",
              "Formoterol",
              "Indacaterol",
              "Theophylline",
              "Omalizumab",
              "Mepolizumab").diff(df_all.columns).foreach(col => {
                df_all = df_all.withColumn(col, lit(0))
              })

            df_all = df_all
              .withColumnRenamed("pm25_avg", "AvgDailyPM2.5Exposure")
              .withColumnRenamed("pm25_max", "MaxDailyPM2.5Exposure")
              .withColumnRenamed("o3_avg", "AvgDailyOzoneExposure")
              .withColumnRenamed("o3_max", "MaxDailyOzoneExposure")
              .withColumnRenamed("pm25_daily_average", "AvgDailyPM2.5Exposure_2")
              .withColumnRenamed("ozone_daily_8hour_maximum", "MaxDailyOzoneExposure_2")

            val deidentify = df_all.columns.intersect(config.deidentify)
            val df_all_visit0 = df_all.drop(deidentify : _*)
            val df_all_visit = df_all_visit0
              .withColumnRenamed("AsthmaDx", "AsthmaDxVisit")
              .withColumnRenamed("CroupDx","CroupDxVisit")
              .withColumnRenamed("ReactiveAirwayDx","ReactiveAirwayDxVisit")
              .withColumnRenamed("CoughDx","CoughDxVisit")
              .withColumnRenamed("PneumoniaDx","PneumoniaDxVisit")
              .withColumnRenamed("ObesityDx","ObesityDxVisit")
              .withColumnRenamed("ObesityBMI", "ObesityBMIVisit")
              .withColumnRenamed("`AvgDailyPM2.5Exposure`","Avg24hPM2.5Exposure")
              .withColumnRenamed("`MaxDailyPM2.5Exposure`","Max24hPM2.5Exposure")
              .withColumnRenamed("AvgDailyOzoneExposure","Avg24hOzoneExposure")
              .withColumnRenamed("MaxDailyOzoneExposure","Max24hOzoneExposure")
              .withColumnRenamed("`AvgDailyPM2.5Exposure_2`","Avg24hPM2.5Exposure_2")
              .withColumnRenamed("MaxDailyOzoneExposure_2","Max24hOzoneExposure_2")
              .withColumnRenamed("Prednisone","PrednisoneVisit")
              .withColumnRenamed("Fluticasone","FluticasoneVisit")
              .withColumnRenamed("Mometasone","MometasoneVisit")
              .withColumnRenamed("Budesonide","BudesonideVisit")
              .withColumnRenamed("Beclomethasone","BeclomethasoneVisit")
              .withColumnRenamed("Ciclesonide","CiclesonideVisit")
              .withColumnRenamed("Flunisolide","FlunisolideVisit")
              .withColumnRenamed("Albuterol","AlbuterolVisit")
              .withColumnRenamed("Metaproterenol","MetaproterenolVisit")
              .withColumnRenamed("Diphenhydramine","DiphenhydramineVisit")
              .withColumnRenamed("Fexofenadine","FexofenadineVisit")
              .withColumnRenamed("Cetirizine","CetirizineVisit")
              .withColumnRenamed("Ipratropium","IpratropiumVisit")
              .withColumnRenamed("Salmeterol","SalmeterolVisit")
              .withColumnRenamed("Arformoterol","ArformoterolVisit")
              .withColumnRenamed("Formoterol","FormoterolVisit")
              .withColumnRenamed("Indacaterol","IndacaterolVisit")
              .withColumnRenamed("Theophylline","TheophyllineVisit")
              .withColumnRenamed("Omalizumab","OmalizumabVisit")
              .withColumnRenamed("Mepolizumab","MepolizumabVisit")


            writeDataframe(hc, output_all_visit, df_all_visit)

            val df_all2 = df_all.groupBy("patient_num", "year").agg(
              first(df_all.col("AgeStudyStart")).alias("AgeStudyStart"),
              first(df_all.col("Sex")).alias("Sex"),
              first(df_all.col("Race")).alias("Race"),
              first(df_all.col("Ethnicity")).alias("Ethnicity"),
              max(df_all.col("AsthmaDx")).alias("AsthmaDx"),
              max(df_all.col("CroupDx")).alias("CroupDx"),
              max(df_all.col("ReactiveAirwayDx")).alias("ReactiveAirwayDx"),
              max(df_all.col("CoughDx")).alias("CoughDx"),
              max(df_all.col("PneumoniaDx")).alias("PneumoniaDx"),
              max(df_all.col("ObesityDx")).alias("ObesityDx"),
              max(df_all.col("ObesityBMI")).alias("ObesityBMI"),
              avg(df_all.col("`AvgDailyPM2.5Exposure`")).alias("AvgDailyPM2.5Exposure"),
              avg(df_all.col("`MaxDailyPM2.5Exposure`")).alias("MaxDailyPM2.5Exposure"),
              avg(df_all.col("AvgDailyOzoneExposure")).alias("AvgDailyOzoneExposure"),
              avg(df_all.col("MaxDailyOzoneExposure")).alias("MaxDailyOzoneExposure"),
              avg(df_all.col("`AvgDailyPM2.5Exposure_2`")).alias("AvgDailyPM2.5Exposure_2"),
              avg(df_all.col("MaxDailyOzoneExposure_2")).alias("MaxDailyOzoneExposure_2"),
              first(df_all.col("EstResidentialDensity")).alias("EstResidentialDensity"),
              first(df_all.col("EstResidentialDensity25Plus")).alias("EstResidentialDensity25Plus"),
              first(df_all.col("EstProbabilityNonHispWhite")).alias("EstProbabilityNonHispWhite"),
              first(df_all.col("EstProbabilityHouseholdNonHispWhite")).alias("EstProbabilityHouseholdNonHispWhite"),
              first(df_all.col("EstProbabilityHighSchoolMaxEducation")).alias("EstProbabilityHighSchoolMaxEducation"),
              first(df_all.col("EstProbabilityNoAuto")).alias("EstProbabilityNoAuto"),
              first(df_all.col("EstProbabilityNoHealthIns")).alias("EstProbabilityNoHealthIns"),
              first(df_all.col("EstProbabilityESL")).alias("EstProbabilityESL"),
              first(df_all.col("EstHouseholdIncome")).alias("EstHouseholdIncome"),
              first(df_all.col("MajorRoadwayHighwayExposure")).alias("MajorRoadwayHighwayExposure"),
              new TotalEDInpatientVisits()(df_all.col("VisitType")).alias("TotalEDInpatientVisits"),
              max(df_all.col("Prednisone")).alias("Prednisone"),
              max(df_all.col("Fluticasone")).alias("Fluticasone"),
              max(df_all.col("Mometasone")).alias("Mometasone"),
              max(df_all.col("Budesonide")).alias("Budesonide"),
              max(df_all.col("Beclomethasone")).alias("Beclomethasone"),
              max(df_all.col("Ciclesonide")).alias("Ciclesonide"),
              max(df_all.col("Flunisolide")).alias("Flunisolide"),
              max(df_all.col("Albuterol")).alias("Albuterol"),
              max(df_all.col("Metaproterenol")).alias("Metaproterenol"),
              max(df_all.col("Diphenhydramine")).alias("Diphenhydramine"),
              max(df_all.col("Fexofenadine")).alias("Fexofenadine"),
              max(df_all.col("Cetirizine")).alias("Cetirizine"),
              max(df_all.col("Ipratropium")).alias("Ipratropium"),
              max(df_all.col("Salmeterol")).alias("Salmeterol"),
              max(df_all.col("Arformoterol")).alias("Arformoterol"),
              max(df_all.col("Formoterol")).alias("Formoterol"),
              max(df_all.col("Indacaterol")).alias("Indacaterol"),
              max(df_all.col("Theophylline")).alias("Theophylline"),
              max(df_all.col("Omalizumab")).alias("Omalizumab"),
              max(df_all.col("Mepolizumab")).alias("Mepolizumab")
            )
            val deidentify2 = df_all2.columns.intersect(config.deidentify)
            val df_all_patient = df_all2.drop(deidentify2 : _*)
            writeDataframe(hc, output_all_patient, df_all_patient)
          }


        }
      case None =>
    }


    spark.stop()
  }

}
