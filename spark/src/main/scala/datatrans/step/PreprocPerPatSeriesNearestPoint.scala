package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import scopt._
import io.circe._
import io.circe.generic.semiauto._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocPerPatSeriesNearestPointConfig(
  patgeo_data : String,
  nearestpoint_data : String,
  output_file : String
)

object PreprocPerPatSeriesNearestPoint extends StepImpl {

  type ConfigType = PreprocPerPatSeriesNearestPointConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: PreprocPerPatSeriesNearestPointConfig): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val hc = spark.sparkContext.hadoopConfiguration
      val output_file_path = new Path(config.output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        println(config.output_file + " exists")
      } else {
        val pddf0 = spark.read.format("csv").option("header", value = true).load(config.patgeo_data)
        val rows = pddf0.mapPartitions(partition => {
          val nearestRoad = new NearestPoint(config.nearestpoint_data)
          partition.map(r => {
            (r.getString(0), nearestRoad.getDistanceToNearestPoint(r.getString(1).toDouble, r.getString(2).toDouble))
          })
        })

        val df = rows.toDF("patient_num", "CAFOExposure")

        writeDataframe(hc, config.output_file, df)
      }

    }

  }

}
