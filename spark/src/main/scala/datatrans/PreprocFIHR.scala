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
  resc_types : Seq[String] = Seq(),
  skip_preproc : Seq[String] = Seq(),
  replace_pat : Boolean = false,
  verify_dups: Boolean = false
)

object PreprocFIHR {

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocFIHRConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_dir").required.action((x,c) => c.copy(input_dir = x))
      opt[String]("output_dir").required.action((x,c) => c.copy(output_dir = x))
      opt[Seq[String]]("resc_types").required.action((x,c) => c.copy(resc_types = x))
      opt[Seq[String]]("skip_preproc").required.action((x,c) => c.copy(skip_preproc = x))
      opt[Unit]("replace_pat").action((x,c) => c.copy(replace_pat = true))
      opt[Unit]("verify_dups").action((x,c) => c.copy(verify_dups = true))
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
    if (!config.skip_preproc.contains(resc_type)) {
      import Implicits0._
      import Implicits1._
      val count = new AtomicInteger(0)
      val n = resc_count(config, hc, input_dir_file_system, resc_type)

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

        println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + id)

        val output_file = config.output_dir + "/" + resc_type + "/" + patient_num + "/" + encodePath(id)
        val output_file_path = new Path(output_file)
        def parseFile : JsValue =
          resc_type match {
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
        def writeFile(obj2 : JsValue) : Unit =
          Utils.writeToFile(hc, output_file, Json.stringify(obj2))
        
        if (output_dir_file_system.exists(output_file_path)) {
          println(output_file + " exists")
          if (config.verify_dups) {
            var duplicate = false
            var obj3 : JsValue = null
            val obj2 = parseFile
            try {
              val output_file_input_stream = output_dir_file_system.open(output_file_path)
              obj3 = Json.parse(output_file_input_stream)
              if(obj3 != obj2) {
                duplicate = true
              }
            } catch {
              case e: Exception =>
                println("caught exception while verifying dups: " + e + ".\n overwriting file " + output_file)
                writeFile(obj2)
            }
            if(duplicate) {
              throw new RuntimeException("differet objects share the same id " + obj3 + obj2)
            }
          }
        } else {
          val obj2 = parseFile
          writeFile(obj2)
        }

      })
    }
  }

  private def resc_count(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, resc_type: String) : Int = {
    val input_dir = config.input_dir + "/" + resc_type
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
        count += (obj \ "entry").get.as[JsArray].value.size
      }
    }
    count
  }

  private def combine_pat(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    import Implicits0._
    import Implicits1._
    val resc_type = "Patient"
    val count = new AtomicInteger(0)

    val n = resc_count(config, hc, input_dir_file_system, resc_type)

    proc_gen(config, hc, input_dir_file_system, resc_type, output_dir_file_system, obj => {
      val pat = obj.as[Patient]
      val patient_num = pat.id
      try {

        println("processing " + resc_type + " " + count.incrementAndGet + " / " + n + " " + patient_num)

        val output_file = config.output_dir + "/" + patient_num
        val output_file_path = new Path(output_file)
        if (!config.replace_pat && output_dir_file_system.exists(output_file_path)) {
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
                try {
                  val input_resc_file_input_stream = output_dir_file_system.open(input_resc_file_path)
                  val obj_resc = Json.parse(input_resc_file_input_stream)
                  arr +:= obj_resc
                } catch {
                  case e : Exception =>
                    throw new Exception("error processing " + resc_type + " " + input_resc_file_path, e)
                }
              }
              obj_pat ++= Json.obj(
                resc_type -> arr
              )
            }
          })

          Utils.writeToFile(hc, output_file, Json.stringify(obj_pat))
        }
      } catch {
        case e : Exception =>
          throw new Exception("error processing Patient " + patient_num, e)
      }

    })

  }

}
