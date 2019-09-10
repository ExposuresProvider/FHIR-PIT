package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._


case class Patient(
  id : String,
  race : Seq[String],
  ethnicity : Seq[String],
  gender : String,
  birthDate : String,
  address: Seq[Address],
  encounter : Seq[Encounter],
  medication : Seq[Medication], // some medications don't have valid encounter id, add them here
  condition : Seq[Condition], // some conditions don't have valid encounter id, add them here
  lab : Seq[Lab], // some lab don't have valid encounter id, add them here
  procedure : Seq[Procedure], // some procedures don't have valid encounter id, add them here
  bmi : Seq[BMI] // some bmis don't have valid encounter id, add them here
)

case class Address(
  lat : Double,
  lon : Double
)

case class Encounter(id : String, subjectReference : String, classAttr : Option[Coding], startDate : Option[String], endDate : Option[String], condition: Seq[Condition], lab: Seq[Lab], medication: Seq[Medication], procedure: Seq[Procedure], bmi: Seq[BMI])

sealed trait Resource {
  val id : String
  val subjectReference : String
  val contextReference : Option[String]
}

case class Condition(override val id : String, override val subjectReference : String, override val contextReference : Option[String], coding: Seq[Coding], assertedDate : String) extends Resource
case class Lab(override val id : String, override val subjectReference : String, override val contextReference : Option[String], coding : Seq[Coding], value : Value, flag : Option[String], effectiveDateTime : String) extends Resource
case class Medication(override val id : String, override val subjectReference : String, override val contextReference : Option[String], coding : Seq[Coding], authoredOn : Option[String], start: String, end: Option[String]) extends Resource
case class Procedure(override val id : String, override val subjectReference : String, override val contextReference : Option[String], coding : Seq[Coding], performedDateTime : String) extends Resource
case class BMI(override val id : String, override val subjectReference : String, override val contextReference : Option[String], coding : Seq[Coding], value : Value) extends Resource

case class Coding(system: String, code: String, display: Option[String])

sealed trait Value
case class ValueQuantity(valueNumber : Double, unit : Option[String]) extends Value
case class ValueString(valueText: String) extends Value


object Implicits{
  implicit val codingCodec: JsonValueCodec[Coding] = JsonCodecMaker.make[Coding](CodecMakerConfig())
  implicit val valueCodec: JsonValueCodec[Value] = JsonCodecMaker.make[Value](CodecMakerConfig())
  implicit val resourceCodec: JsonValueCodec[Resource] = JsonCodecMaker.make[Resource](CodecMakerConfig())
  implicit val encounterCodec: JsonValueCodec[Encounter] = JsonCodecMaker.make[Encounter](CodecMakerConfig())
  implicit val patientCodec: JsonValueCodec[Patient] = JsonCodecMaker.make[Patient](CodecMakerConfig())

  implicit val quantityReads: Reads[Value] = new Reads[Value] {
    override def reads(json: JsValue): JsResult[Value] = {
      json \ "valueQuantity" match {
        case JsDefined(vq) =>
          val value = (vq \ "value").as[Double]
          val unit = (vq \ "code").asOpt[String]
          JsSuccess(ValueQuantity(value, unit))
        case JsUndefined() =>
          JsSuccess(ValueString((json \ "valueString").as[String]))
      }
    }
  }
  implicit val codingReads: Reads[Coding] = new Reads[Coding] {
    override def reads(json: JsValue): JsResult[Coding] = {
      val code = (json \ "code").as[String]
      // set system to empty string if code is 99999
      val system = if(code == "99999") "" else (json \ "system").as[String]
      val display = (json \ "display").asOpt[String]
      JsSuccess(Coding(system, code, display))
    }
  }
  implicit val addressReads: Reads[Address] = new Reads[Address] {
    override def reads(json: JsValue): JsResult[Address] = {
      val latlon = (json \ "extension").as[Seq[JsValue]]
      val lat = (latlon.filter(json => (json \ "url").as[String].toLowerCase == "latitude")(0) \ "valueDecimal").as[Double]
      val lon = (latlon.filter(json => (json \ "url").as[String].toLowerCase == "longitude")(0) \ "valueDecimal").as[Double]
      JsSuccess(Address(lat, lon))
    }
  }
  implicit val patientReads: Reads[Patient] = new Reads[Patient] {
    override def reads(json: JsValue): JsResult[Patient] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val extension = (resource \ "extension").asOpt[Seq[JsValue]]
      val race = extension.map(x => x.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/v3/Race").map(json => (json \ "valueString").as[String])).getOrElse(Seq())
      val ethnicity = extension.map(x => x.filter(json => (json \ "url").as[String] == "http://hl7.org/fhir/v3/Ethnicity").map(json => (json \ "valueString").as[String])).getOrElse(Seq())
      val gender = resource \ "gender" match {
        case JsDefined(x) => x.as[String]
        case JsUndefined() => "Unknown"
      }
      val birthDate = (resource \ "birthDate").as[String]
      println("address = " + (resource \ "address") + " extensions = " + (resource \ "address").asOpt[Seq[JsValue]].map(x=>x.flatMap(x => (x \ "extension").as[Seq[Address]])).getOrElse(Seq()))
      val address = (resource \ "address").asOpt[Seq[JsValue]].map(x=>x.flatMap(x => (x \ "extension").as[Seq[Address]])).getOrElse(Seq())
      JsSuccess(Patient(id, race, ethnicity, gender, birthDate, address, Seq(), Seq(), Seq(), Seq(), Seq(), Seq()))
    }
  }
  implicit val conditionReads: Reads[Condition] = new Reads[Condition] {
    override def reads(json: JsValue): JsResult[Condition] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[Coding]]
      val assertedDate = (resource \ "assertedDate").as[String]
      JsSuccess(Condition(id, subjectReference, contextReference, coding, assertedDate))
    }
  }
  implicit val encounterReads: Reads[Encounter] = new Reads[Encounter] {
    override def reads(json: JsValue): JsResult[Encounter] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val classAttr = (resource \ "class").asOpt[Coding]
      val period = resource \ "period"
      val startDate = (period \ "start").asOpt[String]
      val endDate = (period \ "end").asOpt[String]
      JsSuccess(Encounter(id, subjectReference, classAttr, startDate, endDate, Seq(), Seq(), Seq(), Seq(), Seq()))
    }
  }
  implicit val labReads: Reads[Lab] = new Reads[Lab] {
    override def reads(json: JsValue): JsResult[Lab] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[Coding]]
      val value = resource.as[Value]
      val flag = None
      val effectiveDateTime = (resource \ "effectiveDateTime").asOpt[String].getOrElse((resource \ "issued").as[String])
      JsSuccess(Lab(id, subjectReference, contextReference, coding, value, flag, effectiveDateTime))
    }
  }
  implicit val bmiReads: Reads[BMI] = new Reads[BMI] {
    override def reads(json: JsValue): JsResult[BMI] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[Coding]]
      val value = resource.as[Value]
      JsSuccess(BMI(id, subjectReference, contextReference, coding, value))
    }
  }
  implicit val medicationReads: Reads[Medication] = new Reads[Medication] {
    override def reads(json: JsValue): JsResult[Medication] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "medicationCodeableConcept" \ "coding").as[Seq[Coding]]
      val authoredOn = (resource \ "authoredOn").asOpt[String]
      val validityPeriod = resource \ "dispenseRequest" \ "validityPeriod"
      val start = (validityPeriod \ "start").as[String]
      val end = (validityPeriod \ "end").asOpt[String]
      JsSuccess(Medication(id, subjectReference, contextReference, coding, authoredOn, start, end))
    }
  }
  implicit val procedureReads: Reads[Procedure] = new Reads[Procedure] {
    override def reads(json: JsValue): JsResult[Procedure] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").asOpt[String]
      val coding = (resource \ "code" \ "coding").as[Seq[Coding]]
      val performedDateTime = (resource \ "performedDateTime").as[String]
      JsSuccess(Procedure(id, subjectReference, contextReference, coding, performedDateTime))
    }
  }

}

