package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets


case class Patient(
  id : String,
  race : Seq[String],
  ethnicity : Seq[String],
  gender : String,
  birthDate : String,
  lat : Double,
  lon : Double,
  encounter : Seq[Encounter],
  medication : Seq[Medication], // some medications don't have valid encounter id, add them here
  condition : Seq[Condition], // some conditions don't have valid encounter id, add them here
  lab : Seq[Lab], // some lab don't have valid encounter id, add them here
  procedure : Seq[Procedure], // some procedures don't have valid encounter id, add them here
  bmi : Seq[BMI] // some bmis don't have valid encounter id, add them here
)

case class Encounter(id : String, subjectReference : String, code : Option[String], startDate : Option[String], endDate : Option[String], condition: Seq[Condition], lab: Seq[Lab], medication: Seq[Medication], procedure: Seq[Procedure], bmi: Seq[BMI])

sealed trait Resource {
  val id : String
  val subjectReference : String
  val contextReference : Option[String]
}

case class Condition(override val id : String, override val subjectReference : String, override val contextReference : Option[String], system : String, code : String, assertedDate : String) extends Resource
case class Lab(override val id : String, override val subjectReference : String, override val contextReference : Option[String], code : String, value : Value, flag : Option[String], effectiveDateTime : String) extends Resource
case class Medication(override val id : String, override val subjectReference : String, override val contextReference : Option[String], medication : String, authoredOn : String, start: String, end: Option[String]) extends Resource
case class Procedure(override val id : String, override val subjectReference : String, override val contextReference : Option[String], system : String, code : String, performedDateTime : String) extends Resource
case class BMI(override val id : String, override val subjectReference : String, override val contextReference : Option[String], code : String, value : Value) extends Resource

abstract class Value extends Serializable
case class ValueQuantity(valueNumber : Double, unit : Option[String]) extends Value
case class ValueString(valueText: String) extends Value

object Implicits0 {
  implicit val valueWrites: Writes[Value] = new Writes[Value] {
    override def writes(v : Value) =
      v match {
        case vq : ValueQuantity => Json.toJson(vq)(valueQuantityWrites)
        case vs : ValueString => Json.toJson(vs)(valueStringWrites)
      }
  }
  implicit val valueQuantityWrites: Writes[ValueQuantity] = Json.writes[ValueQuantity]
  implicit val valueStringWrites: Writes[ValueString] = Json.writes[ValueString]

  implicit val conditionWrites: Writes[Condition] = Json.writes[Condition]
  implicit val labWrites: Writes[Lab] = Json.writes[Lab]
  implicit val bmiWrites: Writes[BMI] = Json.writes[BMI]
  implicit val medicationWrites: Writes[Medication] = Json.writes[Medication]
  implicit val procedureWrites: Writes[Procedure] = Json.writes[Procedure]
  implicit val encounterWrites: Writes[Encounter] = Json.writes[Encounter]
  implicit val patientWrites: Writes[Patient] = Json.writes[Patient]
}

object Implicits2 {
  implicit val valueQuantityReads: Reads[ValueQuantity] = Json.reads[ValueQuantity]
  implicit val valueStringReads: Reads[ValueString] = Json.reads[ValueString]

  implicit val valueReads: Reads[Value] = valueQuantityReads.map(a => a.asInstanceOf[Value]) orElse valueStringReads.map(a => a.asInstanceOf[Value])

  implicit val conditionReads: Reads[Condition] = Json.reads[Condition]
  implicit val labReads: Reads[Lab] = Json.reads[Lab]
  implicit val bmiReads: Reads[BMI] = Json.reads[BMI]
  implicit val medicationReads: Reads[Medication] = Json.reads[Medication]
  implicit val procedureReads: Reads[Procedure] = Json.reads[Procedure]
  implicit val encounterReads: Reads[Encounter] = Json.reads[Encounter]
  implicit val patientReads: Reads[Patient] = Json.reads[Patient]
}
object Implicits1 {
  implicit val patientReads: Reads[Patient] = new Reads[Patient] {
    override def reads(json: JsValue): JsResult[Patient] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val extension = (resource \ "extension").as[Seq[JsValue]]
      val race = extension.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/v3/Race").map(json => (json \ "valueString").as[String])
      val ethnicity = extension.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/v3/Ethnicity").map(json => (json \ "valueString").as[String])
      val gender = resource \ "gender" match {
        case JsDefined(x) => x.as[String]
        case JsUndefined() => "Unknown"
      }
      val birthDate = (resource \ "birthDate").as[String]
      val geo = extension.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/StructureDefinition/geolocation")
      assert(geo.size == 1, id)
      val latlon = (geo(0) \ "extension").as[Seq[JsValue]]
      val lat = (latlon.filter(json => (json \ "url").as[String] == "latitude")(0) \ "valueDecimal").as[Double]
      val lon = (latlon.filter(json => (json \ "url").as[String] == "longitude")(0) \ "valueDecimal").as[Double]
      JsSuccess(Patient(id, race, ethnicity, gender, birthDate, lat, lon, Seq(), Seq(), Seq(), Seq(), Seq(), Seq()))
    }
  }
  implicit val conditionReads: Reads[Condition] = new Reads[Condition] {
    override def reads(json: JsValue): JsResult[Condition] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[JsValue]]
      assert(coding.size == 1)
      val system = (coding(0) \ "system").as[String]
      val code = (coding(0) \ "code").as[String]
      val assertedDate = (resource \ "assertedDate").as[String]
      JsSuccess(Condition(id, subjectReference, contextReference, system, code, assertedDate))
    }
  }
  implicit val encounterReads: Reads[Encounter] = new Reads[Encounter] {
    override def reads(json: JsValue): JsResult[Encounter] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val code = (resource \ "class").toOption.map(obj => (obj \ "code").as[String])
      val period = resource \ "period"
      val startDate = (period \ "start").asOpt[String]
      val endDate = (period \ "end").asOpt[String]
      JsSuccess(Encounter(id, subjectReference, code, startDate, endDate, Seq(), Seq(), Seq(), Seq(), Seq()))
    }
  }
  implicit val labReads: Reads[Lab] = new Reads[Lab] {
    override def reads(json: JsValue): JsResult[Lab] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[JsValue]]
      assert(coding.size == 1)
      val code = (coding(0) \ "code").as[String]
      val valueQuantity = resource \ "valueQuantity"
      val value = valueQuantity match {
        case JsDefined(vq) =>
          val value = (vq \ "value").as[Double]
          val unit = (vq \ "code").asOpt[String]
          ValueQuantity(value, unit)
        case JsUndefined() =>
          ValueString((resource \ "valueString").as[String])
      }
      val flag = None
      val effectiveDateTime = (resource \ "effectiveDateTime").as[String]
      JsSuccess(Lab(id, subjectReference, contextReference, code, value, flag, effectiveDateTime))
    }
  }
  implicit val bmiReads: Reads[BMI] = new Reads[BMI] {
    override def reads(json: JsValue): JsResult[BMI] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[JsValue]]
      assert(coding.size == 1)
      val code = (coding(0) \ "code").as[String]
      val valueQuantity = resource \ "valueQuantity"
      val value = valueQuantity match {
        case JsDefined(vq) =>
          val value = (vq \ "value").as[Double]
          val unit = (vq \ "code").asOpt[String]
          ValueQuantity(value, unit)
        case JsUndefined() =>
          ValueString((resource \ "valueString").as[String])
      }
      JsSuccess(BMI(id, subjectReference, contextReference, code, value))
    }
  }
  implicit val medicationReads: Reads[Medication] = new Reads[Medication] {
    override def reads(json: JsValue): JsResult[Medication] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val medication = (resource \ "medicationReference" \ "reference").as[String]
      val authoredOn = (resource \ "authoredOn").as[String]
      val validityPeriod = resource \ "dispenseRequest" \ "validityPeriod"
      val start = (validityPeriod \ "start").as[String]
      val end = (validityPeriod \ "end").asOpt[String]
      JsSuccess(Medication(id, subjectReference, contextReference, medication, authoredOn, start, end))
    }
  }
  implicit val procedureReads: Reads[Procedure] = new Reads[Procedure] {
    override def reads(json: JsValue): JsResult[Procedure] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[JsValue]]
      assert(coding.size == 1)
      val system = (coding(0) \ "system").as[String]
      val code = (coding(0) \ "code").as[String]
      val performedDateTime = (resource \ "performedDateTime").as[String]
      JsSuccess(Procedure(id, subjectReference, contextReference, system, code, performedDateTime))
    }
  }
}


