package datatrans.step

import datatrans.Utils._
import org.apache.spark.sql.SparkSession
import datatrans.Config._
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import datatrans.Implicits._
import datatrans._

object Noop extends StepConfigConfig {

  type ConfigType = Unit

  val yamlFormat = UnitYamlFormat

  val configType = classOf[Unit].getName()

  def step(spark: SparkSession, config: Unit): Unit = ()

}
