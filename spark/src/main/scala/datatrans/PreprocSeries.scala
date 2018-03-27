package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

import scopt._

case class PreprocSeriesConfig(
                                patient_dimension: Option[String] = None,
                                patient_num_list: Option[Seq[String]] = None,
                                input_directory: String = "",
                                output_file: String = ""
                              )

object PreprocSeries {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocSeriesConfig]("series_to_vector") {
      head("series")
      opt[String]("input_directory").required.action((x, c) => c.copy(input_directory = x))
      opt[String]("output_file").required.action((x, c) => c.copy(output_file = x))
    }

    parser.parse(args, PreprocSeriesConfig()) match {
      case Some(config) =>
        time {
          val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

          spark.sparkContext.setLogLevel("WARN")

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._

          val hc = spark.sparkContext.hadoopConfiguration

          val pdif = config.input_directory + "/patient_dimension.csv"
          val vdif = config.input_directory + "/visit_dimension.csv"
          val ofif = config.input_directory + "/observation_fact.csv"
          val geodata_input_filename = config.input_directory + "/geodata.csv"

          println("loading patient_dimension from " + pdif)
          val pddf = spark.read.format("csv").option("header", true).load(pdif)
          println("loading observation_fact from " + ofif)
          val ofdf = spark.read.format("csv").option("header", true).load(ofif)
          println("loading visit_dimension from " + vdif)
          val vddf = spark.read.format("csv").option("header", true).load(vdif)
          println("loading geodata from " + geodata_input_filename)
          val geodata_df = spark.read.format("csv").option("header", true).load(geodata_input_filename)

          val pat = pddf.select("race_cd", "sex_cd", "birth_date")

          val lat = geodata_df.filter($"concept_cd".like("GEOLAT")).select("nval_num","patient_num").groupBy("patient_num").agg(avg("nval_num").as("lat"))

          val lon = geodata_df.filter($"concept_cd".like("GEOLONG")).select("nval_num","patient_num").groupBy("patient_num").agg(avg("nval_num").as("lon"))

          val features = pat
            .join(lat, "patient_num")
            .join(lon, "patient_num")

          // observation
          val observation_cols = Seq(
            "valueflag_cd",
            "valtype_cd",
            "nval_num",
            "tval_char",
            "units_cd",
            "start_date",
            "end_date"
          )
          val observation = ofdf.select("patient_num", "encounter_num", "concept_cd", "instance_num", "modifier_cd", "valueflag_cd", "valtype_cd", "nval_num", "tval_char", "units_cd", "start_date", "end_date").orderBy("start_date")

          val observation_wide = groupByAndAggregate(observation, Seq("patient_num"), Seq("encounter_num", "concept_cd", "instance_num", "modifier_cd"), observation_cols, "observation")

          // visit
          val visit_cols = Seq(
            "inout_cd",
            "start_date",
            "end_date"
          )
          val visit = vddf.select("encounter_num", "inout_cd", "start_date", "end_date").orderBy("start_date")

          val visit_wide = groupByAndAggregate(visit, Seq("patient_num"), Seq("encounter_num"), visit_cols, "visit")

          //      val merge_map = udf((a:Map[String,Any], b:Map[String,Any]) => mergeMap(a,b))

          val features_wide = features
            .join(observation_wide, "patient_num")
            .join(visit_wide, "patient_num")
          //        .select($"patient_num", $"encounter_num", $"sex_cd", $"race_cd", $"birth_date", merge_map($"observation", $"visit"))

          writeCSV(spark, features_wide, config.output_file, JSON)

        }
      case None =>
    }
  }
}
