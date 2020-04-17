package datatrans.step

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession, DataFrame}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time._
import scopt._
import org.apache.spark.sql.functions._
import net.jcazevedo.moultingyaml._
import datatrans.Utils._
import datatrans.ConditionMapper._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocCSVTablePythonConfig(
  input_dir : String = "",
  output_dir : String = "",
  start_date : DateTime = new DateTime(0),
  end_date : DateTime = new DateTime(0),
  deidentify : Seq[String] = Seq(),
  offset_hours : Int = 1
) extends StepConfig

object CSVTablePythonYamlProtocol extends SharedYamlProtocol {
  implicit val csvTablePythonYamlFormat = yamlFormat6(PreprocCSVTablePythonConfig)
}

object PreprocCSVTablePython extends StepConfigConfig {
  
  type ConfigType = PreprocCSVTablePythonConfig

  val yamlFormat = CSVTablePythonYamlProtocol.csvTablePythonYamlFormat

  val configType = classOf[PreprocCSVTablePythonConfig].getName()

  def step(spark: SparkSession, config:PreprocCSVTablePythonConfig) : Unit = {
    
  }

}
