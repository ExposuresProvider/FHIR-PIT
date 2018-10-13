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
  gender : String,
  birthDate : String,
  lat : Double,
  lon : Double,
  encounter : Seq[Encounter]
)

case class Encounter(id : String, subjectReference : String, code : Option[String], startDate : Option[String], endDate : Option[String], condition: Seq[Condition], labs: Seq[Labs], medication: Seq[Medication], procedure: Seq[Procedure])

sealed trait Resource {
  val id : String
  val subjectReference : String
  val contextReference : String
}

case class Condition(override val id : String, override val subjectReference : String, override val contextReference : String, system : String, code : String, assertedDate : String) extends Resource
case class Labs(override val id : String, override val subjectReference : String, override val contextReference : String, code : String, value : Value) extends Resource
case class Medication(override val id : String, override val subjectReference : String, override val contextReference : String, medication : String, authoredOn : String, start: String, end: Option[String]) extends Resource
case class Procedure(override val id : String, override val subjectReference : String, override val contextReference : String, system : String, code : String, performedDateTime : String) extends Resource

abstract class Value
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
  implicit val labsWrites: Writes[Labs] = Json.writes[Labs]
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
  implicit val labsReads: Reads[Labs] = Json.reads[Labs]
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
      val gender = (resource \ "gender").as[String]
      val birthDate = (resource \ "birthDate").as[String]
      val geo = extension.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/StructureDefinition/geolocation")
      assert(geo.size == 1)
      val latlon = (geo(0) \ "extension").as[Seq[JsValue]]
      val lat = (latlon.filter(json => (json \ "url").as[String] == "latitude")(0) \ "valueDecimal").as[Double]
      val lon = (latlon.filter(json => (json \ "url").as[String] == "longitude")(0) \ "valueDecimal").as[Double]
      JsSuccess(Patient(id, race, gender, birthDate, lat, lon, Seq()))
    }
  }
  implicit val conditionReads: Reads[Condition] = new Reads[Condition] {
    override def reads(json: JsValue): JsResult[Condition] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").as[String]
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
      JsSuccess(Encounter(id, subjectReference, code, startDate, endDate, Seq(), Seq(), Seq(), Seq()))
    }
  }
  implicit val labsReads: Reads[Labs] = new Reads[Labs] {
    override def reads(json: JsValue): JsResult[Labs] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").as[String]
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
      JsSuccess(Labs(id, subjectReference, contextReference, code, value))
    }
  }
  implicit val medicationReads: Reads[Medication] = new Reads[Medication] {
    override def reads(json: JsValue): JsResult[Medication] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").as[String]
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
      val contextReference = (resource \ "context" \ "reference").as[String]
      val coding = (resource \ "code" \ "coding").as[Seq[JsValue]]
      assert(coding.size == 1)
      val system = (coding(0) \ "system").as[String]
      val code = (coding(0) \ "code").as[String]
      val performedDateTime = (resource \ "performedDateTime").as[String]
      JsSuccess(Procedure(id, subjectReference, contextReference, system, code, performedDateTime))
    }
  }
}


