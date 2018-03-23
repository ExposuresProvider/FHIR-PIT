package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scopt._

case class PreprocPerPatSeriesConfig(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : String = "",
                   output_prefix : String = ""
                 )

object PreprocPerPatSeries {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesConfig]("series_to_vector") {
      head("series")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = x))
    }

    parser.parse(args, PreprocPerPatSeriesConfig()) match {
      case Some(config) =>
        time {
          val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

          spark.sparkContext.setLogLevel("WARN")

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._

          val hc = spark.sparkContext.hadoopConfiguration

          def proc_pid(p:String) =
            time {

              println("processing pid " + p)

              val pdif = config.input_directory + "/patient_dimension/patient_num=" + p + ".csv"
              val vdif = config.input_directory + "/visit_dimension/patient_num=" + p + ".csv"
              val ofif = config.input_directory + "/observation_fact/patient_num=" + p + ".csv"

              println("loading patient_dimension from " + pdif)
              val pddf = spark.read.format("csv").option("header", true).load(pdif)
              println("loading observation_fact from " + ofif)
              val ofdf = spark.read.format("csv").option("header", true).load(ofif)

              val pat = pddf.select("race_cd", "sex_cd", "birth_date")

              val lat = ofdf.filter($"concept_cd".like("GEO:LAT")).select("nval_num").agg(avg("nval_num").as("lat"))

              val lon = ofdf.filter($"concept_cd".like("GEO:LONG")).select("nval_num").agg(avg("nval_num").as("lon"))

              val features = pat
                .crossJoin(lat)
                .crossJoin(lon)

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
              val observation = ofdf.select("encounter_num", "concept_cd", "instance_num", "modifier_cd", "valueflag_cd", "valtype_cd", "nval_num", "tval_char", "units_cd", "start_date", "end_date").orderBy("start_date")

              val observation_wide = aggregate(observation, Seq("encounter_num", "concept_cd", "instance_num", "modifier_cd"), observation_cols, "observation")

              // visit
              println("loading visit_dimension from " + vdif)
              val vdif_path = new Path(vdif)
              val vdif_fs = vdif_path.getFileSystem(hc)
              val vddf = if(vdif_fs.exists(vdif_path)) {
                spark.read.format("csv").option("header", true).load(vdif)
              } else {
                val schema = StructType(Seq(
                  StructField("inout_cd", StringType, true),
                  StructField("start_date", StringType, true),
                  StructField("end_date", StringType, true)
                ))

                spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
              }
              val visit_cols = Seq(
                "inout_cd",
                "start_date",
                "end_date"
              )
              val visit = vddf.select("encounter_num", "inout_cd", "start_date", "end_date").orderBy("start_date")

              val visit_wide = aggregate(visit, Seq("encounter_num"), visit_cols, "visit")

              //      val merge_map = udf((a:Map[String,Any], b:Map[String,Any]) => mergeMap(a,b))

              val features_wide = features
                  .crossJoin(observation_wide)
                  .crossJoin(visit_wide)
              //        .select($"patient_num", $"encounter_num", $"sex_cd", $"race_cd", $"birth_date", merge_map($"observation", $"visit"))

              // https://stackoverflow.com/questions/41601874/how-to-convert-row-to-json-in-spark-2-scala
              val json = features_wide.toJSON.first()

              val hc = spark.sparkContext.hadoopConfiguration
              writeToFile(hc, config.output_prefix + p, json)

            }

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", true).load(pdif)

                  val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

                  patl.foreach(proc_pid)
                case None =>
              }
          }

          spark.stop()

        }
      case None =>
    }
  }
}
