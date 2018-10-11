package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

case class PreprocPerPatSeriesACSConfig(
                   patient_dimension : Option[String] = None,
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
                 )

object PreprocPerPatSeriesACS {

  def proc_pid(config : PreprocPerPatSeriesACSConfig, spark: SparkSession, nearestRoad: GeoidFinder, p:String): Option[(String, String)] =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = f"${config.time_series}/$p"
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

      if(!input_file_file_system.exists(input_file_path)) {
        println("json not found, skipped " + p)
        None
      } else {
        import Implicits2._
        println("loading json from " + input_file)
        val input_file_input_stream = input_file_file_system.open(input_file_path)


        val patient = Json.parse(input_file_input_stream).as[Patient]
        input_file_input_stream.close()
        val lat = patient.lat
        val lon = patient.lon
        Some((p, nearestRoad.getGeoidForLatLon(lat, lon)))
      }
    }

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesACSConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").required.action((x,c) => c.copy(patient_dimension = Some(x)))
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

          val hc = spark.sparkContext.hadoopConfiguration
          val pddf0 = config.patient_dimension match {
            case Some(pd) =>
              println("loading patient_dimension from " + pd)
          
              spark.read.format("csv").option("header", value = true).load(pd)
            case None =>
              import spark.implicits._
              val input_file = config.time_series
              val input_file_path = new Path(input_file)
              val input_file_file_system = input_file_path.getFileSystem(hc)
              val files = input_file_file_system.listStatus(input_file_path, new PathFilter {
                override def accept(path: Path) : Boolean = input_file_file_system.isFile(path)
              }).map(fs => fs.getPath.getName)
              files.toSeq.toDF("patient_num")

          }
          val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList

          val count = new AtomicInteger(0)
          val n = patl.size
          val output_file_path = new Path(config.output_file)
          val output_file_file_system = output_file_path.getFileSystem(hc)

          if(output_file_file_system.exists(output_file_path)) {
            println(config.output_file + " exists")
          } else {

            val geoidFinder = new GeoidFinder(f"${config.geoid_data}", "15000US")
            val rows = patl.par.flatMap(pid => {
              println("processing " + count.incrementAndGet + " / " + n + " " + pid)
              proc_pid(config, spark, geoidFinder, pid)
            })

            val df = rows.toList.toDF("patient_num", "GEOID")

            val acs_df = spark.read.format("csv").option("header", value = true).load(f"${config.acs_data}")

            val table = df.join(acs_df, "GEOID")

            writeDataframe(hc, config.output_file, table)
          }


        }
      case None =>
    }


  spark.stop()


  }

}
