package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import scopt._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import io.circe._
import io.circe.generic.semiauto._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocPerPatSeriesACSConfig(
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
)

object PreprocPerPatSeriesACS extends StepImpl {

  type ConfigType = PreprocPerPatSeriesACSConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: PreprocPerPatSeriesACSConfig) : Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

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
        }).toDF("patient_num", "GEOID")

        val acs_df = spark.read.format("csv").option("header", value = true).load(config.acs_data)

        val table = Mapper.acs.foldLeft(df.join(acs_df, "GEOID")) {
          case (df, (oldcolumnname, newcolumnname)) => df.withColumnRenamed(oldcolumnname, newcolumnname)
        }

        writeDataframe(hc, config.output_file, table)
      }

    }
    
  }

}
