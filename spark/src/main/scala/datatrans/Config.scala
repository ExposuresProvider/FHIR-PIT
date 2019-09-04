package datatrans

import scopt._
import net.jcazevedo.moultingyaml._

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

}
