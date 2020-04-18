package datatrans.step

import java.io._

import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time._
import org.apache.spark.sql.SparkSession
import io.circe._
import io.circe.generic.semiauto._
import scopt._
import datatrans.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._
import datatrans.PythonUtils._

case class PreprocCSVTablePythonConfig(
  input_dir : String = "",
  output_dir : String = "",
  start_date : DateTime = new DateTime(0),
  end_date : DateTime = new DateTime(0),
  deidentify : Seq[String] = Seq(),
  offset_hours : Int = 1
)

object PreprocCSVTablePython extends StepImpl {
  
  type ConfigType = PreprocCSVTablePythonConfig

  import datatrans.SharedImplicits._

  implicit val configDecoder : Decoder[ConfigType] = deriveDecoder

  implicit val configEncoder : Encoder[ConfigType] = deriveEncoder
  
  def step(spark: SparkSession, config:PreprocCSVTablePythonConfig) : Unit = {
    val (exitValue, out, err) = runPython("csvTable.py", config)
    if (exitValue != 0) {
      throw new RuntimeException(err)
    }
  }

}
