package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils.{JSON, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import play.api.libs.json._
import org.joda.time._
import play.api.libs.json.Json.JsValueWrapper
import org.apache.spark.sql.functions.udf
import scopt._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PreprocPerPatSeriesACSConfig(
                   patient_dimension : String = "",
                   input_directory : String = "",
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
                 )

object PreprocPerPatSeriesACS {

  def proc_pid(config : PreprocPerPatSeriesACSConfig, spark: SparkSession, nearestRoad: GeoidFinder, p:String): Option[(String, String)] =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = f"${config.input_directory}/${config.time_series}/$p"
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

      if(!input_file_file_system.exists(input_file_path)) {
        println("json not found, skipped " + p)
        None
      } else {
        println("loading json from " + input_file)
        val input_file_input_stream = input_file_file_system.open(input_file_path)


        val jsvalue = Json.parse(input_file_input_stream)
        input_file_input_stream.close()

        val lat = jsvalue("lat").as[Double]
        val lon = jsvalue("lon").as[Double]

        Some((p, nearestRoad.getGeoidForLatLon(lat,lon)))

      }
    }

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesACSConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").required.action((x,c) => c.copy(patient_dimension = x))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("acs_data").required.action((x,c) => c.copy(acs_data = x))
      opt[String]("geoid_data").required.action((x,c) => c.copy(geoid_data = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesACSConfig()) match {
      case Some(config) =>

        time {

          println("loading patient_dimension from " + config.patient_dimension)
          val pddf0 = spark.read.format("csv").option("header", value = true).load(config.input_directory + "/" + config.patient_dimension)
          val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList

          val count = new AtomicInteger(0)
          val n = patl.size
          val hc = spark.sparkContext.hadoopConfiguration
          val output_file_path = new Path(config.output_file)
          val output_file_file_system = output_file_path.getFileSystem(hc)

          if(output_file_file_system.exists(output_file_path)) {
            println(config.output_file + " exists")
          } else {

            val geoidFinder = new GeoidFinder(f"${config.input_directory}/${config.geoid_data}")
            val rows = patl.flatMap(pid => {
              println("processing " + count.incrementAndGet + " / " + n + " " + pid)
              proc_pid(config, spark, geoidFinder, pid)
            })

            val lrows = for (row <- rows) yield f"${row._1},${row._2}"

            val table0 = "patient_num,GEOID\n" + lrows.mkString("\n")
            writeToFile(hc, config.output_file+".table", table0)

            val df = rows.toDF("patient_num", "GEOID")

            val acs_df = spark.read.format("csv").option("header", value = true).load(f"${config.input_directory}/${config.acs_data}")


            val table = df.join(acs_df, "GEOID")

            writeDataframe(hc, config.output_file, table)
          }


        }
      case None =>
    }


  spark.stop()


  }

}
