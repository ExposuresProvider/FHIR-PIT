package datatrans.step

import datatrans.Utils._
import org.apache.spark.sql.SparkSession
import datatrans.Config._
import io.circe._
import io.circe.generic.semiauto._
import datatrans.Implicits._
import datatrans._

object Noop extends StepImpl {

  type ConfigType = Unit

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: Unit): Unit = ()

}
