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

case class PreprocPerPatSeriesRowColConfig(
                   patient_dimension : String = "",
                   time_series : String = "",
                   year : Int = 2010,
                   output_file : String = ""
                 )

object PreprocPerPatSeriesACS {

  def proc_pid(config : PreprocPerPatSeriesRowColConfig, spark: SparkSession, year: Int, p:String): Option[(String, Int, Int)] =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = f"${config.time_series}/$p"
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
        jsvalue \ "lat" match {
          case JsUndefined() =>
            println("lat doesn't exists")
            None
          case JsDefined(latitutestr) =>
            val lat = latitutestr.as[Double]
            jsvalue \ "lon" match {
              case JsUndefined() =>
                println("lon doesn't exists")
                None
              case JsDefined(lons) =>
                val lon = lons.as[Double]
                Utils.latlon2rowcol(lat, lon, year).map{case (row, col) => (p, row, col)}
            }
        }

      }
    }

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesRowColConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").required.action((x,c) => c.copy(patient_dimension = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("year").required.action((x,c) => c.copy(year = x.toInt))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesRowColConfig()) match {
      case Some(config) =>

        time {

          println("loading patient_dimension from " + config.patient_dimension)
          val pddf0 = spark.read.format("csv").option("header", value = true).load(config.patient_dimension)
          val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList

          val count = new AtomicInteger(0)
          val n = patl.size
          val hc = spark.sparkContext.hadoopConfiguration
          val output_file_path = new Path(config.output_file)
          val output_file_file_system = output_file_path.getFileSystem(hc)

          if(output_file_file_system.exists(output_file_path)) {
            println(config.output_file + " exists")
          } else {

            val rows = patl.flatMap(pid => {
              println("processing " + count.incrementAndGet + " / " + n + " " + pid)
              proc_pid(config, spark, config.year, pid)
            })

            val df = rows.toDF("patient_num", "row", "col")

            writeDataframe(hc, config.output_file, df)
          }


        }
      case None =>
    }


  spark.stop()


  }

}
