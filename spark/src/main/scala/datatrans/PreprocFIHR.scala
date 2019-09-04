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

import Implicits._

import PreprocFHIRResourceType._

object PreprocFIHR {

  def main(args: Array[String]) {    

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import MyYamlProtocol._

    parseInput[PreprocFIHRConfig](args) match {
      case Some(config) =>
        step(spark, config)
      case None =>
    }


    spark.stop()


  }

  def step(spark: SparkSession, config: PreprocFIHRConfig): Unit = {
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
      val encounter_ids = load_encounter_ids(input_dir_file_system, config.input_directory, config.resc_types(EncounterResourceType))
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

  private def proc_gen(input_dir_file_system: FileSystem, input_dir0: String, resc_dir: String, proc : ((JsObject, String, Int)) => Unit) : Unit = {
    val input_dir = input_dir0 + "/" + resc_dir
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, false)
    while(itr.hasNext) {
      val input_file_path = itr.next().getPath()
      val input_file_input_stream = input_dir_file_system.open(input_file_path)

      println("loading " + input_file_path.getName)

      val obj = Json.parse(input_file_input_stream)

      if (!(obj \ "resourceType").isDefined) {
        proc((obj.as[JsObject], input_file_path.getName, 0))
      } else {
        if((obj \ "entry").isDefined) {
          val entry = (obj \ "entry").get.as[List[JsObject]]
          val n = entry.size

          entry.par.zipWithIndex.map({case (o,i) => (o,input_file_path.getName,i)}).foreach(proc)
        } else {
          println("cannot find entry field " + input_file_path.getName)
        }
      }
    }
  }

  private def proc_resc(config: PreprocFIHRConfig, hc: Configuration, encounter_ids: Seq[String], input_dir_file_system: FileSystem, resc_type: ResourceType, output_dir_file_system: FileSystem) {
    val count = new AtomicInteger(0)
    val resc_dir = config.resc_types(resc_type)
    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj1, f, i) =>
        val obj : Resource = resc_type.fromJson(obj1).asInstanceOf[Resource]

        val id = obj.id
        val patient_num = obj.subjectReference.split("/")(1)
        val encounter_id = obj.contextReference.map(_.split("/")(1))

        println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + id)

        val valid_encounter_id = encounter_id.filter(eid => if (encounter_ids.contains(eid)) true else {
          println("invalid encounter id " + eid)
          false
        })

        val output_file = valid_encounter_id match {
          case Some(eid) => config.output_directory + "/" + resc_type + "/" + patient_num + "/" + eid + "/" + f + "@" + i
          case None => config.output_directory + "/" + resc_type + "/" + patient_num + "/" + f + "@" + i
        }

        val output_file_path = new Path(output_file)
        if (output_dir_file_system.exists(output_file_path)) {
          println(id ++ " file " ++ output_file + " exists")
        } else {
          Utils.saveJson(hc, output_file_path, obj)
        }

    })
  }

  private def proc_enc(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    val resc_type = EncounterResourceType
    val resc_dir = config.resc_types(resc_type)
    val count = new AtomicInteger(0)
    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj1, f, i) =>
        val obj = obj1.as[Encounter]

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

      val obj = Json.parse(input_file_input_stream)

      if (!(obj \ "resourceType").isDefined) {
        count += 1
      } else {
        if((obj \ "entry").isDefined) {
          count += (obj \ "entry").get.as[JsArray].value.size
        } else {
          println("cannot find entry field " + input_file_path.getName)
        }
      }
    }
    count
  }

  private def load_encounter_ids(input_dir_file_system: FileSystem, input_dir0: String, resc_dir: String) : Seq[String] = {
    val input_dir = input_dir0 + "/" + resc_dir
    val input_dir_path = new Path(input_dir)
    val itr = input_dir_file_system.listFiles(input_dir_path, false)
    val encounter_ids = ListBuffer[String]()
    while(itr.hasNext) {
      val input_file_name = itr.next().getPath().getName()
      encounter_ids.append(input_file_name)
    }
    encounter_ids
  }

  private def combine_pat(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    val resc_type = PatientResourceType
    val resc_dir = config.resc_types(resc_type)
    val count = new AtomicInteger(0)

    val n = resc_count(input_dir_file_system, config.input_directory, resc_dir)

    proc_gen(input_dir_file_system, config.input_directory, resc_dir, {
      case (obj, f, i) =>
        var pat = obj.as[Patient]
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
                          val input_resc_file_input_stream = output_dir_file_system.open(input_resc_file_path)
                          Json.parse(input_resc_file_input_stream)
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
            // medication
            val input_med_dir = config.output_directory + "/" + config.resc_types(MedicationRequestResourceType) + "/" + patient_num
            val input_med_dir_path = new Path(input_med_dir)
            val meds = ListBuffer[Medication]()
            if(output_dir_file_system.exists(input_med_dir_path)) {
              Utils.HDFSCollection(hc, input_med_dir_path).foreach(med_dir => {
                val med = Utils.loadJson[Resource](hc, med_dir).asInstanceOf[Medication]
                meds += med
              })
            }
            pat = pat.copy(medication = meds)
            // conds
            val input_cond_dir = config.output_directory + "/" + config.resc_types(ConditionResourceType) + "/" + patient_num
            val input_cond_dir_path = new Path(input_cond_dir)
            val conds = ListBuffer[Condition]()
            if(output_dir_file_system.exists(input_cond_dir_path)) {
              Utils.HDFSCollection(hc, input_cond_dir_path).foreach(cond_dir => {
                val cond = Utils.loadJson[Resource](hc, cond_dir).asInstanceOf[Condition]
                conds += cond
              })
            }
            pat = pat.copy(condition = conds)
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
  private def gen_geodata(spark: SparkSession, config: PreprocFIHRConfig, hc: Configuration, output_dir_file_system: FileSystem) {
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
