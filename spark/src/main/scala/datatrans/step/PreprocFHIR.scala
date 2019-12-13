package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.apache.spark.sql.SparkSession
import cats.syntax.either._
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import scala.collection.mutable.ListBuffer
import scopt._
import java.util.Base64
import java.io.InputStream
import java.nio.charset.StandardCharsets
import datatrans.Config._
import net.jcazevedo.moultingyaml._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import datatrans.Implicits._
import datatrans._

object PreprocFHIRResourceType {
  import Utils._
  sealed trait JsonifiableType {
    type JsonType
    def fromJson(obj : Json):JsonType
  }
  sealed trait ResourceType extends JsonifiableType {
    def setEncounter(enc: Encounter, objs: Seq[Resource]): Encounter
  }
  case object EncounterResourceType extends JsonifiableType {
    type JsonType = Encounter
    override def fromJson(obj : Json): JsonType =
      decode[JsonType](obj)
    override def toString() = "Encounter"
  }
  case object PatientResourceType extends JsonifiableType {
    type JsonType = Patient
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def toString() = "Patient"
  }
  case object LabResourceType extends ResourceType {
    type JsonType = Lab
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def setEncounter(enc: Encounter, objs: Seq[Resource]) : Encounter =
      enc.copy(lab = objs.map(obj => obj.asInstanceOf[Lab]))
    override def toString() = "Lab"
  }
  case object ConditionResourceType extends ResourceType {
    type JsonType = Condition
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def setEncounter(enc: Encounter, objs: Seq[Resource]) : Encounter =
      enc.copy(condition = objs.map(obj => obj.asInstanceOf[Condition]))
    override def toString() = "Condition"
  }
  case object MedicationRequestResourceType extends ResourceType {
    type JsonType = Medication
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def setEncounter(enc: Encounter, objs: Seq[Resource]) : Encounter =
      enc.copy(medication = objs.map(obj => obj.asInstanceOf[Medication]))
    override def toString() = "MedicationRequest"
  }
  case object ProcedureResourceType extends ResourceType {
    type JsonType = Procedure
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def setEncounter(enc: Encounter, objs: Seq[Resource]) : Encounter =
      enc.copy(procedure = objs.map(obj => obj.asInstanceOf[Procedure]))
    override def toString() = "Procedure"
  }
  case object BMIResourceType extends ResourceType {
    type JsonType = Lab
    override def fromJson(obj : Json):JsonType =
      decode[JsonType](obj)
    override def setEncounter(enc: Encounter, objs: Seq[Resource]) : Encounter =
      enc.copy(bmi = objs.map(obj => obj.asInstanceOf[Lab]))
    override def toString() = "BMI"
  }

}

import PreprocFHIRResourceType._

case class PreprocFHIRConfig(
  input_directory : String = "", // input directory of FHIR data
  output_directory : String = "", // output directory of patient data
  resc_types : Map[JsonifiableType, String], // map resource type to directory, these are resources included in patient data
  skip_preproc : Seq[String] // skip preprocessing these resource as they have already benn preprocessed
) extends StepConfig

object FHIRYamlProtocol extends DefaultYamlProtocol {
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
  implicit val fhirYamlFormat = yamlFormat4(PreprocFHIRConfig)
}

object PreprocFHIR extends StepConfigConfig {

  import Utils._

  def main(args: Array[String]) {    

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import FHIRYamlProtocol._

    parseInput[PreprocFHIRConfig](args) match {
      case Some(config) =>
        step(spark, config)
      case None =>
    }

    spark.stop()

  }

  type ConfigType = PreprocFHIRConfig

  val yamlFormat = FHIRYamlProtocol.fhirYamlFormat

  val configType = classOf[PreprocFHIRConfig].getName()

  def step(spark: SparkSession, config: PreprocFHIRConfig): Unit = {
    Utils.time {

      val hc = spark.sparkContext.hadoopConfiguration
      val input_dir_path = new Path(config.input_directory)
      val input_dir_file_system = input_dir_path.getFileSystem(hc)

      val output_dir_path = new Path(config.output_directory)
      val output_dir_file_system = output_dir_path.getFileSystem(hc)

      println("processing Encounter")
      if (!config.skip_preproc.contains(EncounterResourceType.toString)) {
        proc_enc(config, hc, input_dir_file_system, output_dir_file_system)
      }

      println("loading Encounter ids")
      val encounter_ids = load_encounter_ids(output_dir_file_system, config.output_directory, config.resc_types(EncounterResourceType))
      println("processing Resources")
      config.resc_types.keys.foreach(resc_type =>
        resc_type match {
          case ty : ResourceType =>
            if(!config.skip_preproc.contains(resc_type.toString))
              proc_resc(config, hc, encounter_ids, input_dir_file_system, resc_type.asInstanceOf[ResourceType], output_dir_file_system)
          case _ =>
        }
      )
      println("combining Patient")
      combine_pat(config, hc, input_dir_file_system, output_dir_file_system)
      println("generating geodata")
      gen_geodata(spark, config, hc, output_dir_file_system)

    }
  }

  private def parseInputStream(input_file_input_stream : InputStream) : Json = {
    val str = scala.io.Source.fromInputStream(input_file_input_stream).mkString

    parse(str) match {
      case Left(error) => throw new RuntimeException(error)
      case Right(obj) => obj
    }
  }

  private def proc_gen(input_dir_file_system: FileSystem, input_dir0: String, resc_dir: String, proc : ((Json, String, Int)) => Unit) : Unit = {
    val input_dir = input_dir0 + "/" + resc_dir
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, false)
    while(itr.hasNext) {
      val input_file_path = itr.next().getPath()
      val input_file_input_stream = input_dir_file_system.open(input_file_path)

      println("loading " + input_file_path.getName)

      val obj = parseInputStream(input_file_input_stream)
      if (obj.hcursor.downField("resourceType").failed) {
        proc((obj, input_file_path.getName, 0))
      } else {
        val entry0 = obj.hcursor.downField("entry")
        if(entry0.succeeded) {
          for(
            entry <- entry0.as[List[Json]];
            n = entry.size
          ) yield entry.par.zipWithIndex.map({case (o,i) => (o,input_file_path.getName,i)}).foreach(proc)
        } else {
          println("cannot find entry field " + input_file_path.getName)
        }
      }
    }
  }

  private def proc_resc(config: PreprocFHIRConfig, hc: Configuration, encounter_ids: Set[String], input_dir_file_system: FileSystem, resc_type: ResourceType, output_dir_file_system: FileSystem) {
    val count = new AtomicInteger(0)
    val resc_dir = config.resc_types(resc_type)
    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj1, f, i) =>
        println("decoding json " + obj1)
        val obj : Resource = resc_type.fromJson(obj1).asInstanceOf[Resource]

        val id = obj.id
        val patient_num = obj.subjectReference.split("/")(1)
        val encounter_id = obj.contextReference.map(_.split("/")(1))

        println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + id)

        val valid_encounter_id = encounter_id.filter(eid => if (encounter_ids.contains(eid)) true else {
          println("invalid encounter id " + eid + " available " + encounter_ids)
          false
        })

        val output_file = valid_encounter_id match {
          case Some(eid) => config.output_directory + "/" + config.resc_types(resc_type) + "/" + patient_num + "/" + eid + "/" + f + "@" + i
          case None => config.output_directory + "/" + config.resc_types(resc_type) + "/" + patient_num + "/" + f + "@" + i
        }

        val output_file_path = new Path(output_file)
        if (output_dir_file_system.exists(output_file_path)) {
          println(id ++ " file " ++ output_file + " exists")
        } else {
          println("saving json " + obj)
          Utils.saveJson(hc, output_file_path, obj)
        }

    })
  }

  private def proc_enc(config: PreprocFHIRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    val resc_type = EncounterResourceType
    val resc_dir = config.resc_types(resc_type)
    val count = new AtomicInteger(0)
    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj1, f, i) =>
        val obj = decode[Encounter](obj1)
        val id = obj.id
        val patient_num = obj.subjectReference.split("/")(1)

        println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + id)

        val output_file = config.output_directory + "/" + resc_type + "/" + patient_num + "/" + f + "@" + i
        val output_file_path = new Path(output_file)

        if (output_dir_file_system.exists(output_file_path)) {
          println(output_file + " exists")
        } else {
          Utils.saveJson(hc, output_file_path, obj)
        }
        
    })
  }

  private def resc_count(input_dir_file_system: FileSystem, input_dir0: String, resc_dir: String) : Int = {
    val input_dir = input_dir0 + "/" + resc_dir
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, false)
    var count = 0
    while(itr.hasNext) {
      val input_file_path = itr.next().getPath()
      val input_file_input_stream = input_dir_file_system.open(input_file_path)

      println("loading " + input_file_path.getName)

      val obj = parseInputStream(input_file_input_stream)

      if (obj.hcursor.downField("resourceType").failed) {
        count += 1
      } else {
        val entry0 = obj.hcursor.downField("entry")
        if(entry0.succeeded) {
          count += entry0.values.get.size
        } else {
          println("cannot find entry field " + input_file_path.getName)
        }
      }
    }
    count
  }

  private def load_encounter_ids(input_dir_file_system: FileSystem, input_dir0: String, resc_dir: String) : Set[String] = {
    val input_dir = input_dir0 + "/" + resc_dir
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, true)
    val encounter_ids = ListBuffer[String]()
    while(itr.hasNext) {
      val input_file_name = itr.next().getPath().getName()
      println(input_file_name)
      encounter_ids.append(input_file_name.split("@")(1))
    }
    encounter_ids.toSet
  }

  private def combine_pat(config: PreprocFHIRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    val resc_type = PatientResourceType
    val resc_dir = config.resc_types(resc_type)
    val count = new AtomicInteger(0)

    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj, f, i) =>
        var pat = decode[Patient](obj)
        val patient_num = pat.id
        try {

          println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + patient_num)

          val output_file = config.output_directory + "/" + config.resc_types(PatientResourceType) + "/" + patient_num
          val output_file_path = new Path(output_file)
          if (output_dir_file_system.exists(output_file_path)) {
            println(output_file + " exists")
          } else {
            // encounter 
            val input_enc_dir = config.output_directory + "/" + config.resc_types(EncounterResourceType) + "/" + patient_num
            val input_enc_dir_path = new Path(input_enc_dir)
            val encs = ListBuffer[Encounter]()
            if(output_dir_file_system.exists(input_enc_dir_path)) {
              Utils.HDFSCollection(hc, input_enc_dir_path).foreach(encounter_dir => {
                var enc = Utils.loadJson[Encounter](hc, encounter_dir)
                val encounter_id = enc.id
                config.resc_types.keys.foreach{
                  case resc_type : ResourceType => 
                    val input_resc_dir = config.output_directory + "/" + config.resc_types(resc_type) + "/" + patient_num + "/" + encounter_id
                    val input_resc_dir_path = new Path(input_resc_dir)
                    if(output_dir_file_system.exists(input_resc_dir_path)) {
                      println("found resource " + config.resc_types(resc_type) + "/" + patient_num + "/" + encounter_id)
                      val objs = Utils.HDFSCollection(hc, input_resc_dir_path).map(input_resc_file_path =>
                        try {
                          Utils.loadJson[Resource](hc, input_resc_file_path)
                        } catch {
                          case e : Exception =>
                            throw new Exception("error processing " + resc_type + " " + input_resc_file_path, e)
                       }).toSeq
                      enc = resc_type.setEncounter(enc, objs)
                    } else {
                      println("cannot find resource " + config.resc_types(resc_type) + "/" + patient_num + "/" + encounter_id)
                    }
                  case _ =>
                }
                encs += enc
              })
            }
            pat = pat.copy(encounter = encs)
            def combineRescWithoutValidEncounterNumber[R](rt: ResourceType, update: Seq[R] => Unit) = {
              // medication
              val input_med_dir = config.output_directory + "/" + config.resc_types(rt) + "/" + patient_num
              val input_med_dir_path = new Path(input_med_dir)
              val meds = ListBuffer[R]()
              if(output_dir_file_system.exists(input_med_dir_path)) {
                Utils.HDFSCollection(hc, input_med_dir_path).foreach(med_dir => {
                  val med = Utils.loadJson[Resource](hc, med_dir).asInstanceOf[R]
                  meds += med
                })
              }
              update(meds)
            }

            config.resc_types.keys.foreach{
              case MedicationRequestResourceType =>
                combineRescWithoutValidEncounterNumber(MedicationRequestResourceType, (meds : Seq[Medication]) => { pat = pat.copy(medication = meds) })
              case ConditionResourceType =>
                combineRescWithoutValidEncounterNumber(ConditionResourceType, (meds: Seq[Condition]) => { pat = pat.copy(condition = meds) })
              case LabResourceType =>
                combineRescWithoutValidEncounterNumber(LabResourceType, (meds: Seq[Lab]) => { pat = pat.copy(lab = meds) })
              case ProcedureResourceType =>
                combineRescWithoutValidEncounterNumber(ProcedureResourceType, (meds: Seq[Procedure]) => { pat = pat.copy(procedure = meds) })
              case BMIResourceType =>
                combineRescWithoutValidEncounterNumber(BMIResourceType, (meds: Seq[Lab]) => { pat = pat.copy(bmi = meds) })
              case PatientResourceType =>
              case EncounterResourceType =>
            }

            val output_file_path = new Path(output_file)
            Utils.saveJson(hc, output_file_path, pat)
          }
        } catch {
          case e : Exception =>
            throw new Exception("error processing Patient " + patient_num, e)
        }

    })

  }

  case class PatientGeo(patient_num: String, lat: Double, lon: Double)
  private def gen_geodata(spark: SparkSession, config: PreprocFHIRConfig, hc: Configuration, output_dir_file_system: FileSystem) {
    import spark.implicits._

    val resc_type = "Patient"
    val pat_dir = config.output_directory + "/" + config.resc_types(PatientResourceType)
    val pat_dir_path = new Path(pat_dir)
    val pat_dir_df = output_dir_file_system.listStatus(pat_dir_path, new PathFilter {
      override def accept(path : Path): Boolean = output_dir_file_system.isFile(path)
    }).map(fs => fs.getPath.getName).toSeq.toDS

    val out_df = pat_dir_df.map(patient_num => {
      println("processing " + patient_num)
      try {
        val output_file = config.output_directory + "/" + config.resc_types(PatientResourceType) + "/" + patient_num
        val pat = Utils.loadJson[Patient](new Configuration(), new Path(output_file))
        if (pat.address.length == 0) {
          println("no lat lon")
          PatientGeo(patient_num, 0xffff, 0xffff)
        } else {
          if(pat.address.length > 1) {
            println("more than one lat lon using first")
          }
          PatientGeo(patient_num, pat.address(0).lat, pat.address(0).lon)
        }
      } catch {
        case e : Exception =>
          throw new Exception("error processing Patient " + patient_num, e)
      }
    })

    val out_file = config.output_directory + "/geo.csv"
    Utils.writeDataframe(hc, out_file, out_df.toDF())


  }
}
