package datatrans

import scopt._
import io.circe.yaml
import io.circe._
import io.circe.generic.semiauto._
import cats.syntax.either._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.SparkSession
import Implicits._
import datatrans.step._
import scala.reflect.runtime.universe

case class InputConfig(
  config : String = ""
)


object Config {

  private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def getObject(name:String) : Any = 
    runtimeMirror.reflectModule(runtimeMirror.staticModule(name)).instance

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
        parseYAML[T](yamlStr)
      }
    )

  }

  def parseYAML[T](yamlStr: String)(implicit decoder : Decoder[T]) : Option[T] = {
    val obj = yaml.parser.parse(yamlStr)
    if (obj.isRight) {
      val tobj = obj.right.get.as[T]
      if (tobj.isRight)
        Some(tobj.right.get)
      else {
        println(yamlStr)
        val err = tobj.left.get
        println(err)
        None
      }
    } else {
      println(yamlStr)
      val err = obj.left.get
      println(err)
      None
    }
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
  skip: String,
  dependsOn: Seq[Seq[String]]
)



object SharedImplicits {
  val fmt = ISODateTimeFormat.dateTimeNoMillis()
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
        config <- c.downField("arguments").as[impl.ConfigType](impl.configDecoder)
      ) yield config
  }

  implicit val stepDecoder : Decoder[Step] = new Decoder[Step] {
    final def apply(c : HCursor) : Decoder.Result[Step] =
      for(
        name <- c.downField("name").as[String];
        skip <- c.downField("skip").as[String];
        dependsOn <- c.downField("dependsOn").as[Seq[Seq[String]]];
        config <- c.downField("step").as[Any];
        impl <- c.downField("step").as[StepImpl]
      ) yield Step(config, impl, name, skip, dependsOn)
  }

}
