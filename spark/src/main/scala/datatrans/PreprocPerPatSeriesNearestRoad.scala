package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._

case class PreprocPerPatSeriesNearestRoadConfig(
                   patient_dimension : String = "",
                   input_directory : String = "",
                   time_series : String = "",
                   nearestroad_data : String = "",
                   maximum_search_radius : Double = 500,
                   output_file : String = ""
                 )

object PreprocPerPatSeriesNearestRoad {

  def proc_pid(config : PreprocPerPatSeriesNearestRoadConfig, spark: SparkSession, nearestRoad: NearestRoad, p:String): Option[(String, Double)] =
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

                Some((p, nearestRoad.getMinimumDistance(lat, lon)))
            }
        }

      }
    }

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesNearestRoadConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").required.action((x,c) => c.copy(patient_dimension = x))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("nearestroad_data").required.action((x,c) => c.copy(nearestroad_data = x))
      opt[Double]("maximum_search_radius").action((x,c) => c.copy(maximum_search_radius = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesNearestRoadConfig()) match {
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

            val nearestRoad = new NearestRoad(f"${config.input_directory}/${config.nearestroad_data}", config.maximum_search_radius)
            val rows = patl.flatMap(pid => {
              println("processing " + count.incrementAndGet + " / " + n + " " + pid)
              proc_pid(config, spark, nearestRoad, pid)
            })

            val lrows = for (row <- rows) yield f"${row._1},${row._2}"

            val table = "patient_num,distant_to_nearest_road\n" + lrows.mkString("\n")
            writeToFile(hc, config.output_file, table)
          }


        }
      case None =>
    }


  spark.stop()


  }
}
