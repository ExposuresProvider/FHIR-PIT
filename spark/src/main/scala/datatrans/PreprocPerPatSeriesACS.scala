package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

case class PreprocPerPatSeriesACSConfig(
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
                 )

object PreprocPerPatSeriesACS {

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesACSConfig]("series_to_vector") {
      head("preproc acs")
      opt[String]("pat_geo").required.action((x,c) => c.copy(time_series = x))
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
          val output_file_path = new Path(config.output_file)
          val output_file_file_system = output_file_path.getFileSystem(hc)

          if(output_file_file_system.exists(output_file_path)) {
            println(config.output_file + " exists")
          } else {
            val pddf0 = spark.read.format("csv").option("header", value = true).load(config.time_series)

            val geoidFinder = new GeoidFinder(config.geoid_data, "15000US")
            val df = pddf0.map(r => {
              val geoid = geoidFinder.getGeoidForLatLon(r.getDouble(1), r.getDouble(2))
              (r.getString(0), geoid)
            }).toDF("patient_num", "GEOID")

            val acs_df = spark.read.format("csv").option("header", value = true).load(config.acs_data)

            val table = df.join(acs_df, "GEOID")

            writeDataframe(hc, config.output_file, table)
          }

        }
      case None =>
    }

    spark.stop()

  }

}
