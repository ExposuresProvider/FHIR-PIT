package datatrans

import scopt._
import net.jcazevedo.moultingyaml._
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.SparkSession
import Implicits._
import datatrans.step._

case class InputConfig(
  config : String = ""
)

object Config {

  def parseInput[T](args : Seq[String])(implicit configFormatExplicit : YamlFormat[T]) : Option[T] = {

    val parser = new OptionParser[InputConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("config").required.action((x,c) => c.copy(config = x))
    }

    parser.parse(args, InputConfig()).map(
      configFile => {
        val source = scala.io.Source.fromFile(configFile.config)
        val yamlStr = try source.mkString finally source.close()
        yamlStr.parseYaml.convertTo[T]
      }
    )

  }

  val stepConfigConfigMap: Map[String, StepConfigConfig] = Seq(PreprocFHIR, PreprocPerPatSeriesToVector, PreprocPerPatSeriesEnvData, PreprocPerPatSeriesNearestRoad, PreprocPerPatSeriesACS, PreprocPerPatSeriesACS2, PreprocCombineData, Noop).map(c => (c.configType, c)).toMap
}

trait StepConfigConfig {
  type ConfigType
  val yamlFormat : YamlFormat[ConfigType]
  val configType : String
  def step(spark: SparkSession, config: ConfigType)
}

trait StepConfig {
  val configConfig : StepConfigConfig
}


case class Step(
  step: StepConfig,
  name: String,
  skip: Boolean,
  dependsOn: Seq[String]
)



trait SharedYamlProtocol extends DefaultYamlProtocol {
  val fmt = ISODateTimeFormat.dateTime()
  implicit val dateTimeFormat = new YamlFormat[DateTime] {
    def write(x: DateTime) =
      YamlString(fmt.print(x))

    def read(value: YamlValue) =
      value match {
        case YamlString(s) =>
          fmt.parseDateTime(s)
        case _ =>
          throw new RuntimeException("cannot parse date time from YamlValue " + value)
      }

  }

}


object StepYamlProtocol extends DefaultYamlProtocol {

  implicit val configFormat = new YamlFormat[StepConfig] {
    def write(x: StepConfig) = {
      val stepConfigConfig = Config.stepConfigConfigMap(x.configConfig.configType)
      YamlObject(
        YamlString("function") -> YamlString(x.configConfig.configType),
        YamlString("arguments") -> stepConfigConfig.yamlFormat.write(x.asInstanceOf[stepConfigConfig.ConfigType])
      )
    }

    def read(value: YamlValue) = {
      val config = value.asYamlObject.getFields(YamlString("arguments")).head
      val stepConfigConfig = Config.stepConfigConfigMap(value.asYamlObject.getFields(YamlString("function")).head)
      stepConfigConfig.yamlFormat.read(config).asInstanceOf[StepConfig]
    }
  }
  implicit val stepFormat = yamlFormat4(Step)
}
