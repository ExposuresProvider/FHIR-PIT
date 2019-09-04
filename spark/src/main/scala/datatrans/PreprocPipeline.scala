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
  resc_types : Map[JsonifiableType, String] = Map(), // map resource type to directory, these are resources included in patient data
  skip_preproc : Seq[String] = Seq() // skip preprocessing these resource as they have already benn preprocessed
) extends StepConfig

case class PreprocPerPatSeriesToVectorConfig(
  input_directory : String,
  output_directory : String,
  start_date : DateTime,
  end_date : DateTime,
  med_map : Option[String]
) extends StepConfig

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

  implicit val configFormat = new YamlFormat[StepConfig] {
    def write(x: StepConfig) =
      x match {
        case c : PreprocFIHRConfig => YamlObject(
          YamlString("step") -> YamlString("FHIR"),
          YamlString("config") -> preprocFHIRConfigFormat.write(c)
        )
        case c : PreprocPerPatSeriesToVectorConfig => YamlObject(
          YamlString("step") -> YamlString("PerPatSeriesToVector"),
          YamlString("config") -> preprocPetPatSeriesToVectorConfigFormat.write(c)
        )
      }

    def read(value: YamlValue) = {
      val config = value.asYamlObject.getFields(YamlString("config")).head
      value.asYamlObject.getFields(YamlString("step")).head match {
        case YamlString("FHIR") =>
          preprocFHIRConfigFormat.read(config)
        case YamlString("PetPatSeriesToVector") =>
          preprocPetPatSeriesToVectorConfigFormat.read(config)
      }
    }
  }

}


object PreprocPipeline {

  def main(args: Array[String]) {    

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    // import spark.implicits._
    import MyYamlProtocol._

    parseInput[Seq[StepConfig]](args) match {
      case Some(config) =>
        for (conf <- config) {
          println(conf)
          conf match {
            case c : PreprocFIHRConfig =>
              PreprocFIHR.step(spark, c)
            case c : PreprocPerPatSeriesToVectorConfig =>
              PreprocPerPatSeriesToVector.step(spark, c)
          }
        }

      case None =>
    }


    spark.stop()


  }

}
