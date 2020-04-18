package datatrans.step

import datatrans.Utils._
import org.apache.spark.sql.SparkSession
import org.joda.time._
import scopt._
import io.circe._
import io.circe.generic.semiauto._

import datatrans.environmentaldata._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._


case class EnvDataCoordinatesConfig(
  patgeo_data : String,
  environmental_data : String,
  output_dir : String,
  start_date : DateTime,
  end_date : DateTime,
  indices : Seq[String], // = Seq("o3", "pm25"),
  statistics : Seq[String], // = Seq("avg", "max"),
  offset_hours : Int
)

object PreprocPerPatSeriesEnvDataCoordinates extends StepImpl {

  type ConfigType = EnvDataCoordinatesConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: EnvDataCoordinatesConfig) = {
    time {
      val datasource = new EnvDataSource(spark, config)
      datasource.run()
    }
  }
}
