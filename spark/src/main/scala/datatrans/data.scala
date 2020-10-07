package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.syntax.either._
import io.circe._
import io.circe.parser._
import io.circe.optics.JsonPath._


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
  bmi : Seq[Lab] // some bmis don't have valid encounter id, add them here
)

case class Address(
  lat : Double,
  lon : Double
)

case class Encounter(id : String, subjectReference : String, classAttr : Option[Coding], startDate : Option[String], endDate : Option[String], condition: Seq[Condition], lab: Seq[Lab], medication: Seq[Medication], procedure: Seq[Procedure], bmi: Seq[Lab])

sealed trait Resource {
  val id : String
  val subjectReference : String
  val contextReference : Option[String]
  val coding: Seq[Coding]
}

case class Condition(override val id : String, override val subjectReference : String, override val contextReference : Option[String], override val coding: Seq[Coding], assertedDate : String) extends Resource
case class Lab(override val id : String, override val subjectReference : String, override val contextReference : Option[String], override val coding : Seq[Coding], value : Option[Value], flag : Option[String], effectiveDateTime : String) extends Resource
case class Medication(override val id : String, override val subjectReference : String, override val contextReference : Option[String], override val coding : Seq[Coding], authoredOn : Option[String], start: String, end: Option[String]) extends Resource
case class Procedure(override val id : String, override val subjectReference : String, override val contextReference : Option[String], override val coding : Seq[Coding], performedDateTime : String) extends Resource

case class Coding(system: String, code: String, display: Option[String])

sealed trait Value
case class ValueQuantity(valueNumber : Double, unit : Option[String]) extends Value
case class ValueString(valueText: String) extends Value


object Implicits{
  implicit val codingCodec: JsonValueCodec[Coding] = JsonCodecMaker.make[Coding](CodecMakerConfig())
  implicit val valueCodec: JsonValueCodec[Value] = JsonCodecMaker.make[Value](CodecMakerConfig())
  implicit val resourceCodec: JsonValueCodec[Resource] = JsonCodecMaker.make[Resource](CodecMakerConfig())
  implicit val encounterCodec: JsonValueCodec[Encounter] = JsonCodecMaker.make[Encounter](CodecMakerConfig())
  implicit val addressCodec: JsonValueCodec[Address] = JsonCodecMaker.make[Address](CodecMakerConfig())
  implicit val patientCodec: JsonValueCodec[Patient] = JsonCodecMaker.make[Patient](CodecMakerConfig())

  implicit val quantityDecoder: Decoder[Value] = new Decoder[Value] {
    override def apply(json: HCursor): Decoder.Result[Value] = {
      val vq = json.downField("valueQuantity")
      if (vq.succeeded) {
        for(
          value <- vq.downField("value").as[Double];
          unit = vq.downField("code").as[String].right.toOption
        ) yield ValueQuantity(value, unit)
      } else {
        for(
          value <- json.downField("valueString").as[String]
        ) yield ValueString(value)
      }
    }
  }

  implicit val codingDecoder: Decoder[Coding] = new Decoder[Coding] {
    override def apply(json: HCursor): Decoder.Result[Coding] = 
      for (
        code <- json.downField("code").as[String];
        // set system to empty string if code is 99999
        system <- if (code == "99999") Right("") else json.downField("system").as[String];
        display = json.downField("display").as[String].right.toOption
      ) yield Coding(system, code, display)

  }

  def findJsonByUrl(jsons : Iterable[Json], url : String) : Option[Json] =
    jsons.find(json => (for(s <- json.hcursor.downField("url").as[String].map(_.toLowerCase)) yield s == url).getOrElse(false))

  implicit val addressDecoder: Decoder[Address] = new Decoder[Address] {
    override def apply(json: HCursor): Decoder.Result[Address] = 
      for (
        latlon <- json.downField("extension").as[Seq[Json]];
        lat <- findJsonByUrl(latlon, "latitude").get.hcursor.downField("valueDecimal").as[Double];
        lon <- findJsonByUrl(latlon, "longitude").get.hcursor.downField("valueDecimal").as[Double]
      ) yield (Address(lat, lon))
  }

  def toResult[A](a: Option[A]) = a.fold[Either[DecodingFailure, A]](Left(DecodingFailure("error",List())))(x => Right (x))

  implicit val patientDecoder: Decoder[Patient] = new Decoder[Patient] {
    override def apply(json: HCursor): Decoder.Result[Patient] =
      for(
        json <- json.as[Json];
        resource = root.resource;
        id <- toResult(resource.id.string.getOption(json));
        birthDate <- toResult(resource.birthDate.string.getOption(json));
        extension = resource.extension.each.json.getAll(json);
        race =
          (for(
            json <- extension;
            s <- root.url.string.getOption(json).toSeq
            if s == "http://terminology.hl7.org/ValueSet/v3-Race" || s == "http://hl7.org/fhir/v3/Race";
            s <- root.valueString.string.getOption(json).toSeq ++ root.extension.each.valueString.string.getAll(json)
          ) yield s).toSeq;
        ethnicity =
          (for(
            json <- extension;
            s <- root.url.string.getOption(json).toSeq
            if s == "http://terminology.hl7.org/ValueSet/v3-Ethnicity" || s == "http://hl7.org/fhir/v3/Ethnicity";
            s <- root.valueString.string.getOption(json).toSeq ++ root.extension.each.valueString.string.getAll(json)
          ) yield s).toSeq;
        gender = resource.gender.string.getOption(json).getOrElse("Unknown");
        address = resource.address.each.extension.each.json.getAll(json).map(_.as[Address].right.toOption).toSeq.flatten
      ) yield Patient(id, race, ethnicity, gender, birthDate, address, Seq(), Seq(), Seq(), Seq(), Seq(), Seq())
  }

  def filterCoding(coding: Json) : Boolean =
    if(coding.hcursor.downField("code").succeeded) 
      true
    else {
      println(f"cannot find code field in JsValue ${coding}")
      false
    }

  def getCoding(code : ACursor) : Decoder.Result[Seq[Coding]] =
    for(
      codingJson <- code.downField("coding").as[Seq[Json]]
    ) yield (for(
      json <- codingJson
      if filterCoding(json)
    ) yield {
      val codeResult = json.as[Coding]
      codeResult match {
        case Left(error) => return Left(error)
        case Right(code) => code
      }
    }).toSeq

  implicit val conditionDecoder: Decoder[Condition] = new Decoder[Condition] {
    override def apply(json: HCursor): Decoder.Result[Condition] = {
      val resource = json.downField("resource");
      for(
        id <- resource.downField("id").as[String];
        subjectReference <- resource.downField("subject").downField("reference").as[String];
        contextReference = resource.downField("context").downField("reference").as[String].right.toOption;
        coding <- getCoding(resource.downField("code"));
        assertedDate <- resource.downField("assertedDate").as[String]
      ) yield Condition(id, subjectReference, contextReference, coding, assertedDate)
    }
  }

  implicit val encounterDecoder: Decoder[Encounter] = new Decoder[Encounter] {
    override def apply(json: HCursor): Decoder.Result[Encounter] = {
      val resource = json.downField("resource");
      for(
        id <- resource.downField("id").as[String];
        subjectReference <- resource.downField("subject").downField("reference").as[String];
        classAttr = resource.downField("class").as[Coding].right.toOption;
        period = resource.downField("period");
        startDate = period.downField("start").as[String].right.toOption;
        endDate = period.downField("end").as[String].right.toOption
      ) yield Encounter(id, subjectReference, classAttr, startDate, endDate, Seq(), Seq(), Seq(), Seq(), Seq())
    }
  }

  implicit val labDecoder: Decoder[Lab] = new Decoder[Lab] {
    override def apply(json: HCursor): Decoder.Result[Lab] = {
      val resource = json.downField("resource");
      for(
        id <- resource.downField("id").as[String];
        subjectReference <- resource.downField("subject").downField("reference").as[String];
        contextReference = resource.downField("context").downField("reference").as[String].right.toOption;
        coding <- getCoding(resource.downField("code"));
        value = resource.as[Value].right.toOption;
        flag = None;
        effectiveDateTime0 = resource.downField("effectiveDateTime");
        effectiveDateTime <- if (effectiveDateTime0.succeeded) effectiveDateTime0.as[String] else resource.downField("issued").as[String]
      ) yield Lab(id, subjectReference, contextReference, coding, value, flag, effectiveDateTime)
    }
  }

  implicit val medicationDecoder: Decoder[Medication] = new Decoder[Medication] {
    override def apply(json: HCursor): Decoder.Result[Medication] = {
      val resource = json.downField("resource")
      for(
        id <- resource.downField("id").as[String];
        subjectReference <- resource.downField("subject").downField("reference").as[String];
        contextReference = resource.downField("context").downField("reference").as[String].right.toOption;
        coding <- getCoding(resource.downField("medicationCodeableConcept"));
        authoredOn = resource.downField("authoredOn").as[String].right.toOption;
        validityPeriod = resource.downField("dispenseRequest").downField("validityPeriod");
        start <- validityPeriod.downField("start").as[String];
        end = validityPeriod.downField("end").as[String].right.toOption
      ) yield (Medication(id, subjectReference, contextReference, coding, authoredOn, start, end))
    }
  }

  implicit val procedureDecoder: Decoder[Procedure] = new Decoder[Procedure] {
    override def apply(json: HCursor): Decoder.Result[Procedure] = {
      val resource = json.downField("resource")
      for(
        id <- resource.downField("id").as[String];
        subjectReference <- resource.downField("subject").downField("reference").as[String];
        contextReference = resource.downField("context").downField("reference").as[String].right.toOption;
        coding <- getCoding(resource.downField("code"));
        performedDateTime <- resource.downField("performedDateTime").as[String]
      ) yield (Procedure(id, subjectReference, contextReference, coding, performedDateTime))
    }
  }

}

