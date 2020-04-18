package datatrans

import scopt._
import io.circe.yaml
import io.circe._
import io.circe.generic.semiauto._
import cats.syntax.either._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.SparkSession
import Implicits._
import datatrans.step._
import scala.reflect.runtime.universe
import scala.reflect.classTag

case class InputConfig(
  config : String = ""
)


object Config {

  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def getObject(name:String) : Any = {
    val module = runtimeMirror.staticModule("package.ObjectName")

    val obj = runtimeMirror.reflectModule(module)

    obj.instance
  }

  // def getObjectName(obj: Any) : String = obj.getClass().getCanonicalName().dropRight(1)
  def parseInput[T](args : Seq[String])(implicit decoder : Decoder[T]) : Option[T] = {

    val parser = new OptionParser[InputConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("config").required.action((x,c) => c.copy(config = x))
    }

    parser.parse(args, InputConfig()).flatMap(
      configFile => {
        val source = scala.io.Source.fromFile(configFile.config)
        val yamlStr = try source.mkString finally source.close()

        yaml.parser.parse(yamlStr).right.toOption.flatMap(_.as[T].right.toOption)
      }
    )

  }
}

import Config._

trait StepImpl {
  type ConfigType
  val configDecoder : Decoder[ConfigType]
  def step(spark: SparkSession, config: ConfigType)
}

case class Step(
  config: Any,
  impl: StepImpl,
  name: String,
  skip: Boolean,
  dependsOn: Seq[String]
)



object SharedImplicits {
  val fmt = ISODateTimeFormat.dateTime()
  implicit val dateTimeDecoder : Decoder[org.joda.time.DateTime] = new Decoder[org.joda.time.DateTime] {
    final def apply(c: HCursor) : Decoder.Result[org.joda.time.DateTime] =
      for (
        s <- c.as[String]
      ) yield fmt.parseDateTime(s)

  }

  implicit val dateTimeEncoder : Encoder[org.joda.time.DateTime] = new Encoder[org.joda.time.DateTime] {
    final def apply(x: org.joda.time.DateTime) : Json =
      Json.fromString(fmt.print(x))

  }

}


object StepImplicits {



  implicit val stepImplDecoder : Decoder[StepImpl] = new Decoder[StepImpl] {
    final def apply(c : HCursor) : Decoder.Result[StepImpl] =
      for(
        qn <- c.downField("function").as[String]
      ) yield getObject(qn).asInstanceOf[StepImpl]
  }

  // implicit val stepImplEncoder : Encoder[StepImpl] = new Encoder[StepImpl] {
  //   final def apply(x : StepImpl) : Json = Json.fromString(getObjectName(x))
  // }

  implicit val stepConfigDecoder : Decoder[Any] = new Decoder[Any] {
    final def apply(c : HCursor): Decoder.Result[Any] =
      for(
        qn <- c.downField("function").as[String];
        val impl = getObject(qn).asInstanceOf[StepImpl];
        config <- impl.configDecoder(c)
      ) yield config
  }

  implicit val stepDecoder : Decoder[Step] = new Decoder[Step] {
    final def apply(c : HCursor) : Decoder.Result[Step] =
      for(
        name <- c.downField("name").as[String];
        skip <- c.downField("skip").as[Boolean];
        dependsOn <- c.downField("dependsOn").as[Seq[String]];
        config <- c.as[Any];
        impl <- c.as[StepImpl]
      ) yield Step(config, impl, name, skip, dependsOn)
  }

}
