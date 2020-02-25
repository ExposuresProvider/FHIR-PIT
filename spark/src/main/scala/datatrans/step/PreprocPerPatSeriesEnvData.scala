package datatrans.step

import datatrans.Utils._
import org.apache.spark.sql.SparkSession
import org.joda.time._
import scopt._
import net.jcazevedo.moultingyaml._

import datatrans.environmentaldata._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._


case class EnvDataSourceConfig(
  patgeo_data : String,
  environmental_data : String,
  output_file : String,
  start_date : DateTime,
  end_date : DateTime,
  indices : Seq[String], // = Seq("o3", "pm25"),
  statistics : Seq[String], // = Seq("avg", "max"),
  offset_hours : Int
) extends StepConfig

object PreprocPerPatSeriesEnvDataYamlProtocol extends SharedYamlProtocol {
  implicit val preprocPerPatSeriesEnvDataYamlFormat = yamlFormat8(EnvDataSourceConfig)
}

object PreprocPerPatSeriesEnvData extends StepConfigConfig {

  type ConfigType = EnvDataSourceConfig

  val yamlFormat = PreprocPerPatSeriesEnvDataYamlProtocol.preprocPerPatSeriesEnvDataYamlFormat

  val configType = classOf[EnvDataSourceConfig].getName()

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import PreprocPerPatSeriesEnvDataYamlProtocol._

    parseInput[EnvDataSourceConfig](args) match {
      case Some(config) =>
        step(spark, config)
      case None =>
    }

    spark.stop()

  }

  def step(spark: SparkSession, config: EnvDataSourceConfig) = {
    time {
      val datasource = new EnvDataSource(spark, config)
      datasource.run()
    }
  }
}
