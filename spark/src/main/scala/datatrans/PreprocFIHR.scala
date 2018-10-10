package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import java.util.Base64
import java.nio.charset.StandardCharsets

case class PreprocFIHRConfig(
                   input_dir : String = "",
                   output_dir : String = "",
                   resc_types : Seq[String] = Seq()
                 )

case class Patient(
                  id : String,
                  race : Seq[String],
                  gender : String,
                  birthDate : String,
                  lat : Double,
                  lon : Double
                  )

sealed trait Resource {
  val id : String
  val subjectReference : String
}

case class Condition(override val id : String, override val subjectReference : String, contextReference : String, system : String, code : String, assertedDate : String) extends Resource
case class Encounter(override val id : String, override val subjectReference : String, code : Option[String], startDate : Option[String], endDate : Option[String]) extends Resource
case class Labs(override val id : String, override val subjectReference : String, contextReference : String, code : String, value : Double, unit : String) extends Resource
case class Medication(override val id : String, override val subjectReference : String, contextReference : String, medication : String, authoredOn : String) extends Resource
case class Procedure(override val id : String, override val subjectReference : String, contextReference : String, system : String, code : String, performedDateTime : String) extends Resource

object Implicits {
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
      JsSuccess(Patient(id, race, gender, birthDate, lat, lon))
    }
  }
  implicit val patientWrites: Writes[Patient] = Json.writes[Patient]
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
  implicit val conditionWrites: Writes[Condition] = Json.writes[Condition]
  implicit val encounterReads: Reads[Encounter] = new Reads[Encounter] {
    override def reads(json: JsValue): JsResult[Encounter] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val code = (resource \ "class").toOption.map(obj => (obj \ "code").as[String])
      val period = resource \ "period"
      val startDate = (period \ "start").asOpt[String]
      val endDate = (period \ "end").asOpt[String]
      JsSuccess(Encounter(id, subjectReference, code, startDate, endDate))
    }
  }
  implicit val encounterWrites: Writes[Encounter] = Json.writes[Encounter]
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
      val value = (valueQuantity \ "value").as[Double]
      val unit = (valueQuantity \ "unit").as[String]
      JsSuccess(Labs(id, subjectReference, contextReference, code, value, unit))
    }
  }
  implicit val labsWrites: Writes[Labs] = Json.writes[Labs]
  implicit val medicationReads: Reads[Medication] = new Reads[Medication] {
    override def reads(json: JsValue): JsResult[Medication] = {
      val resource = json \ "resource"
      val id = (resource \ "id").as[String]
      val subjectReference = (resource \ "subject" \ "reference").as[String]
      val contextReference = (resource \ "context" \ "reference").as[String]
      val medication = (resource \ "medicationReference" \ "reference").as[String]
      val authoredOn = (resource \ "authoredOn").as[String]
      JsSuccess(Medication(id, subjectReference, contextReference, medication, authoredOn))
    }
  }
  implicit val medicationWrites: Writes[Medication] = Json.writes[Medication]
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
  implicit val procedureWrites: Writes[Procedure] = Json.writes[Procedure]
}


object PreprocFIHR {

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocFIHRConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_dir").required.action((x,c) => c.copy(input_dir = x))
      opt[String]("output_dir").required.action((x,c) => c.copy(output_dir = x))
      opt[Seq[String]]("resc_types").required.action((x,c) => c.copy(resc_types = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    // import spark.implicits._

    parser.parse(args, PreprocFIHRConfig()) match {
      case Some(config) =>

        Utils.time {


          val hc = spark.sparkContext.hadoopConfiguration
          val input_dir_path = new Path(config.input_dir)
          val input_dir_file_system = input_dir_path.getFileSystem(hc)

          val output_dir_path = new Path(config.output_dir)
          val output_dir_file_system = output_dir_path.getFileSystem(hc)

          println("processing Resources")
          config.resc_types.foreach(resc_type => proc_resc(config, hc, input_dir_file_system, resc_type, output_dir_file_system))
          println("combining Patient")
          combine_pat(config, hc, input_dir_file_system, output_dir_file_system)

        }
      case None =>
    }


    spark.stop()


  }

  private def encodePath(a: String) : String =
    Base64.getEncoder.encodeToString(a.getBytes(StandardCharsets.UTF_8))

  private def proc_gen(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, resc_type: String, output_dir_file_system: FileSystem, proc : JsObject => Unit) : Unit = {
    val input_dir = config.input_dir + "/" + resc_type
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, false)
    while(itr.hasNext) {
      val input_file_path = itr.next().getPath()
      val input_file_input_stream = input_dir_file_system.open(input_file_path)

      println("loading " + input_file_path.getName)

      val obj = Json.parse(input_file_input_stream)

      if (!(obj \ "resourceType").isDefined) {
        proc(obj.as[JsObject])
      } else {
        val entry = (obj \ "entry").get.as[List[JsObject]]
        val n = entry.size

        entry.par.foreach(proc)
      }
    }
  }

  private def proc_resc(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, resc_type: String, output_dir_file_system: FileSystem) {
    import Implicits._
    val count = new AtomicInteger(0)

    proc_gen(config, hc, input_dir_file_system, resc_type, output_dir_file_system, obj1 => {
      val obj : Resource = resc_type match {
        case "Condition" =>
          obj1.as[Condition]
        case "Encounter" =>
          obj1.as[Encounter]
        case "Labs" =>
          obj1.as[Labs]
        case "Medication" =>
          obj1.as[Medication]
        case "Procedure" =>
          obj1.as[Procedure]
      }

      val id = obj.id
      val patient_num = obj.subjectReference.split("/")(1)

      println("processing " + resc_type + " " + count.incrementAndGet + " " + id)

      val output_file = config.output_dir + "/" + resc_type + "/" + patient_num + "/" + encodePath(id)
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {
        val obj2 : JsValue = resc_type match {
          case "Condition" =>
            Json.toJson(obj.asInstanceOf[Condition])
          case "Encounter" =>
            Json.toJson(obj.asInstanceOf[Encounter])
          case "Labs" =>
            Json.toJson(obj.asInstanceOf[Labs])
          case "Medication" =>
            Json.toJson(obj.asInstanceOf[Medication])
          case "Procedure" =>
            Json.toJson(obj.asInstanceOf[Procedure])
        }

        Utils.writeToFile(hc, output_file, Json.stringify(obj2))
      }

    })
  }

  private def combine_pat(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    import Implicits._
    val count = new AtomicInteger(0)

    val resc_type = "Patient"

    proc_gen(config, hc, input_dir_file_system, resc_type, output_dir_file_system, obj => {
      var obj1 = obj
      val pat = obj1.as[Patient]
      val patient_num = pat.id

      println("processing " + resc_type + " " + count.incrementAndGet + " " + patient_num)

      val output_file = config.output_dir + "/" + patient_num
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {
        var obj_pat = Json.toJson(pat).as[JsObject]

        config.resc_types.foreach(resc_type => {
          var arr = Json.arr()
          val input_resc_dir = config.output_dir + "/" + resc_type + "/" + patient_num
          val input_resc_dir_path = new Path(input_resc_dir)
          if(output_dir_file_system.exists(input_resc_dir_path)) {
            val input_resc_file_iter = output_dir_file_system.listFiles(input_resc_dir_path, false)
            while(input_resc_file_iter.hasNext) {
              val input_resc_file_status = input_resc_file_iter.next()
              val input_resc_file_path = input_resc_file_status.getPath
              val input_resc_file_input_stream = output_dir_file_system.open(input_resc_file_path)
              val obj_resc = Json.parse(input_resc_file_input_stream)
              arr +:= obj_resc
            }
            obj_pat ++= Json.obj(
              resc_type -> arr
            )
          }
        })

        Utils.writeToFile(hc, output_file, Json.stringify(obj_pat))
      }

    })

  }

}
