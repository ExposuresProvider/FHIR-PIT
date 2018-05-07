package datatrans

import java.util
import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
          val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

          spark.sparkContext.setLogLevel("WARN")

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._

          def proc_pid(p:String): Unit =
            time {

              val hc = spark.sparkContext.hadoopConfiguration
              val output_filename = config.output_prefix + p
              val output_file_path = new Path(output_filename)
              val output_file_fs = output_file_path.getFileSystem(hc)
              if(output_file_fs.exists(output_file_path)) {
                println(output_file_path + " exists")
              } else {

                val pdif = config.input_directory + "/patient_dimension/patient_num=" + p + ".csv"
                val vdif = config.input_directory + "/visit_dimension/patient_num=" + p + ".csv"
                val ofif = config.input_directory + "/observation_fact/patient_num=" + p + ".csv"
                val geodata_input_filename = config.input_directory + "/geodata/patient_num=" + p + ".csv"

                val vdif_path = new Path(vdif)
                val vdif_fs = vdif_path.getFileSystem(hc)
                val ofif_path = new Path(ofif)
                val ofif_fs = ofif_path.getFileSystem(hc)
                val geodata_input_path = new Path(geodata_input_filename)
                val geodata_input_fs = geodata_input_path.getFileSystem(hc)

                println("loading patient_dimension from " + pdif)
                val pddf = spark.read.format("csv").option("header", value = true).load(pdif)

                val pat = pddf.select("race_cd", "sex_cd", "birth_date")

                val emptyObjectLatLon = lit(null).cast(DoubleType)
                val features = if (geodata_input_fs.exists(geodata_input_path)) {
                  println("loading geodata from " + geodata_input_filename)
                  val geodata_df = spark.read.format("csv").option("header", value = true).load(geodata_input_filename)
                  val lat = geodata_df.filter($"concept_cd".like("GEOLAT")).select("nval_num")

                  val lon = geodata_df.filter($"concept_cd".like("GEOLONG")).select("nval_num")

                  val pat2 = if (lat.count != 0) pat.crossJoin(lat.agg(avg("nval_num").as("lat"))) else pat.withColumn("lat", emptyObjectLatLon)
                  if (lon.count != 0) pat2.crossJoin(lon.agg(avg("nval_num").as("lon"))) else pat2.withColumn("lon", emptyObjectLatLon)
                } else
                  pat.withColumn("lat", emptyObjectLatLon).withColumn("lon", emptyObjectLatLon)

                def emptyObjectObervationVisit(col:String) = {

                  import collection.JavaConverters._
                  val bufferSchema: StructType = StructType(
                    Seq(
                      StructField(col, MapType(StringType, StringType, valueContainsNull = false))
                    )
                  )

                  spark.createDataFrame(Seq(Row(Map())).asJava, bufferSchema)
                }


                val observation_wide = if(ofif_fs.exists(ofif_path)) {
                  println("loading observation_fact from " + ofif)
                  val ofdf = spark.read.format("csv").option("header", value = true).load(ofif)
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

                  aggregate(observation, Seq("encounter_num", "concept_cd", "instance_num", "modifier_cd"), observation_cols, "observation")

                } else
                  emptyObjectObervationVisit("obervation")


                val visit_wide = if(vdif_fs.exists(vdif_path)) {
                  println("loading visit_dimension from " + vdif)
                  val vddf = spark.read.format("csv").option("header", value = true).load(vdif)

                  // visit
                  val visit_cols = Seq(
                    "inout_cd",
                    "start_date",
                    "end_date"
                  )
                  val visit = vddf.select("encounter_num", "inout_cd", "start_date", "end_date").orderBy("start_date")

                  aggregate(visit, Seq("encounter_num"), visit_cols, "visit")
                } else
                  emptyObjectObervationVisit("visit")

                //      val merge_map = udf((a:Map[String,Any], b:Map[String,Any]) => mergeMap(a,b))

                val features_wide = features
                  .crossJoin(observation_wide)
                  .crossJoin(visit_wide)
                //        .select($"patient_num", $"encounter_num", $"sex_cd", $"race_cd", $"birth_date", merge_map($"observation", $"visit"))

                // https://stackoverflow.com/questions/41601874/how-to-convert-row-to-json-in-spark-2-scala
                val json = features_wide.toJSON.first()

                writeToFile(hc, output_filename, json)


              }

            }

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", value = true).load(pdif)

                  val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

                  val count = new AtomicInteger(0)
                  val n = patl.size
                  patl.foreach(pid => {
                    println("processing " + count.incrementAndGet + " / " + n + " " + pid)
                    proc_pid(pid)
                  })
                case None =>
              }
          }

          spark.stop()

        }
      case None =>
    }
  }
}
