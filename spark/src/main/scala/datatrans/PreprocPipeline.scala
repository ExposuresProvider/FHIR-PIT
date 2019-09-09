package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scala.collection.mutable.ListBuffer
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets
import datatrans.Config._
import net.jcazevedo.moultingyaml._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import scala.collection.mutable.{Set, Queue}
import scala.util.control._
import Breaks._

import Implicits._

object PreprocFHIRResourceType {
  sealed trait JsonifiableType {
    type JsonType
    def fromJson(obj : JsValue):JsonType
  }
  sealed trait ResourceType extends JsonifiableType {
    def setEncounter(enc: Encounter, objs: Seq[JsValue]): Encounter
  }
  case object EncounterResourceType extends JsonifiableType {
    type JsonType = Encounter
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def toString() = "Encounter"
  }
  case object PatientResourceType extends JsonifiableType {
    type JsonType = Patient
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def toString() = "Patient"
  }
  case object LabResourceType extends ResourceType {
    type JsonType = Lab
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def setEncounter(enc: Encounter, objs: Seq[JsValue]) : Encounter =
      enc.copy(lab = objs.map(obj => obj.as[Lab]))
    override def toString() = "Lab"
  }
  case object ConditionResourceType extends ResourceType {
    type JsonType = Condition
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def setEncounter(enc: Encounter, objs: Seq[JsValue]) : Encounter =
      enc.copy(condition = objs.map(obj => obj.as[Condition]))
    override def toString() = "Condition"
  }
  case object MedicationRequestResourceType extends ResourceType {
    type JsonType = Medication
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def setEncounter(enc: Encounter, objs: Seq[JsValue]) : Encounter =
      enc.copy(medication = objs.map(obj => obj.as[Medication]))
    override def toString() = "MedicationRequest"
  }
  case object ProcedureResourceType extends ResourceType {
    type JsonType = Procedure
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def setEncounter(enc: Encounter, objs: Seq[JsValue]) : Encounter =
      enc.copy(procedure = objs.map(obj => obj.as[Procedure]))
    override def toString() = "Procedure"
  }
  case object BMIResourceType extends ResourceType {
    type JsonType = BMI
    override def fromJson(obj : JsValue):JsonType =
      obj.as[JsonType]
    override def setEncounter(enc: Encounter, objs: Seq[JsValue]) : Encounter =
      enc.copy(bmi = objs.map(obj => obj.as[BMI]))
    override def toString() = "BMI"
  }

}

import PreprocFHIRResourceType._

sealed trait StepConfig
case class PreprocFIHRConfig(
  input_directory : String = "", // input directory of FHIR data
  output_directory : String = "", // output directory of patient data
  resc_types : Map[JsonifiableType, String], // map resource type to directory, these are resources included in patient data
  skip_preproc : Seq[String] // skip preprocessing these resource as they have already benn preprocessed
) extends StepConfig

case class PreprocPerPatSeriesToVectorConfig(
  input_directory : String,
  output_directory : String,
  start_date : DateTime,
  end_date : DateTime,
  med_map : Option[String]
) extends StepConfig

case class EnvDataSourceConfig(
  patgeo_data : String,
  environmental_data : String,
  output_file : String,
  start_date : DateTime,
  end_date : DateTime,
  fips_data: String,
  indices : Seq[String], // = Seq("o3", "pm25"),
  statistics : Seq[String], // = Seq("avg", "max"),
  indices2 : Seq[String] // = Seq("ozone_daily_8hour_maximum", "pm25_daily_average")
) extends StepConfig


case class Step(
  step: StepConfig,
  name: String,
  dependsOn: Seq[String]
)

object MyYamlProtocol extends DefaultYamlProtocol {
  implicit val resourceTypeFormat = new YamlFormat[JsonifiableType] {
    def write(x: JsonifiableType) = StringYamlFormat.write(x.toString())
    def read(value: YamlValue) = StringYamlFormat.read(value) match {
      case "Condition" =>
        ConditionResourceType
      case "Lab" =>
        LabResourceType
      case "MedicationRequest" =>
        MedicationRequestResourceType
      case "Procedure" =>
        ProcedureResourceType
      case "BMI" =>
        BMIResourceType
      case "Encounter" =>
        EncounterResourceType
      case "Patient" =>
        PatientResourceType
      case a =>
        throw new RuntimeException("unsupported resource type " + a)
    }
  }

  implicit val preprocFHIRConfigFormat = yamlFormat4(PreprocFIHRConfig)

  implicit val preprocPetPatSeriesToVectorConfigFormat = yamlFormat5(PreprocPerPatSeriesToVectorConfig)

  implicit val envDataSourceConfigFormat = yamlFormat9(EnvDataSourceConfig)

  implicit val configFormat = new YamlFormat[StepConfig] {
    def write(x: StepConfig) =
      x match {
        case c : PreprocFIHRConfig => YamlObject(
          YamlString("function") -> YamlString("FHIR"),
          YamlString("arguments") -> preprocFHIRConfigFormat.write(c)
        )
        case c : PreprocPerPatSeriesToVectorConfig => YamlObject(
          YamlString("function") -> YamlString("PerPatSeriesToVector"),
          YamlString("arguments") -> preprocPetPatSeriesToVectorConfigFormat.write(c)
        )
        case c : EnvDataSourceConfig => YamlObject(
          YamlString("function") -> YamlString("EnvDataSource"),
          YamlString("arguments") -> envDataSourceConfigFormat.write(c)
        )
      }

    def read(value: YamlValue) = {
      val config = value.asYamlObject.getFields(YamlString("arguments")).head
      value.asYamlObject.getFields(YamlString("function")).head match {
        case YamlString("FHIR") =>
          preprocFHIRConfigFormat.read(config)
        case YamlString("PerPatSeriesToVector") =>
          preprocPetPatSeriesToVectorConfigFormat.read(config)
        case YamlString("EnvDataSource") =>
          envDataSourceConfigFormat.read(config)
        case c =>
          throw new RuntimeException(c)
      }
    }
  }

  implicit val stepFormat = yamlFormat3(Step)

}


sealed trait Status
case object Success extends Status
case object Failure extends Status
case object NotRun extends Status

object PreprocPipeline {


  def safely[T](handler: PartialFunction[Throwable, T]): PartialFunction[Throwable, T] = {
    case ex: ControlThrowable => throw ex
      // case ex: OutOfMemoryError (Assorted other nasty exceptions you don't want to catch)
	
    //If it's an exception they handle, pass it on
    case ex: Throwable if handler.isDefinedAt(ex) => handler(ex)
	
    // If they didn't handle it, rethrow. This line isn't necessary, just for clarity
    case ex: Throwable => throw ex
  }

  def main(args: Array[String]) {    

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    // import spark.implicits._
    import MyYamlProtocol._

    parseInput[Seq[Step]](args) match {
      case Some(steps) =>
        val queued = Queue[Step]()
        val success = Set[String]()
        val failure = Set[(String, Throwable)]()
        val notRun = Set[String]()

        queued.enqueue(steps:_*)
        breakable {
          while(!queued.isEmpty) {
            breakable {
              while (true) {
                queued.dequeueFirst(step => !(step.dependsOn.toSet & (failure.map(_._1) | notRun)).isEmpty) match {
                  case None => break
                  case Some(step) =>
                    notRun.add(step.name)
                    println("notRun: " + step.name)
                }
              }
            }

            queued.dequeueFirst(step => step.dependsOn.toSet.subsetOf(success)) match {
              case None => break
              case Some(step) =>

                println(step)
                try {
                  step.step match {
                    case c : PreprocFIHRConfig =>
                      PreprocFIHR.step(spark, c)
                    case c : PreprocPerPatSeriesToVectorConfig =>
                      PreprocPerPatSeriesToVector.step(spark, c)
                    case c : EnvDataSourceConfig =>
                      PreprocPerPatSeriesEnvData.step(spark, c)
                  }
                  println("success: " + step.name)
                  success.add(step.name)
                } catch safely {
                  case e: Throwable =>
                    failure.add((step.name, e))
                    println("failure: " + step.name + " by " + e)
                }

            }
          }
        }
        queued.foreach(step => notRun.add(step.name))
        val status = Map[Status, Any](
          Success -> success,
          Failure -> failure,
          NotRun -> notRun
        )
        println(status)
      case None =>

    }


    spark.stop()


  }

}
