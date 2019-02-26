package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

case class PreprocPerPatSeriesACS2Config(
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
                 )

object PreprocPerPatSeriesACS2 {

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesACS2Config]("series_to_vector") {
      head("preproc acs")
      opt[String]("patgeo_data").required.action((x,c) => c.copy(time_series = x))
      opt[String]("acs_data").required.action((x,c) => c.copy(acs_data = x))
      opt[String]("geoid_data").required.action((x,c) => c.copy(geoid_data = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesACS2Config()) match {
      case Some(config) =>

        time {

          val hc = spark.sparkContext.hadoopConfiguration

          if(fileExists(hc, config.output_file)) {
            println(config.output_file + " exists")
          } else {
            val pddf0 = spark.read.format("csv").option("header", value = true).load(config.time_series)

            val df = pddf0.mapPartitions(partition => {
              val geoidFinder = new GeoidFinder(config.geoid_data, "15000US")
              partition.map(r => {
                val geoid = geoidFinder.getGeoidForLatLon(r.getString(1).toDouble, r.getString(2).toDouble)
                (r.getString(0), geoid)
              })
            }).toDF("patient_num", "geoid")

            val acs_df = spark.read.format("csv").option("header", value = true).load(config.acs_data).select("geoid", "ur")

            val table = df.join(acs_df, "geoid")

            writeDataframe(hc, config.output_file, table)
          }

        }
      case None =>
    }

    spark.stop()

  }

}
