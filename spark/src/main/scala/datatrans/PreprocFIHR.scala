package datatrans

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession}
import play.api.libs.json._
import scopt._

case class PreprocFIHRConfig(
                   input_dir : String = "",
                   output_dir : String = "",
                   resc_types : Seq[String] = Seq()
                 )

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
    import spark.implicits._

    parser.parse(args, PreprocFIHRConfig()) match {
      case Some(config) =>

        Utils.time {


          val hc = spark.sparkContext.hadoopConfiguration
          val input_dir_path = new Path(config.input_dir)
          val input_dir_file_system = input_dir_path.getFileSystem(hc)

          val output_dir_path = new Path(config.output_dir)
          val output_dir_file_system = output_dir_path.getFileSystem(hc)

          println("processing Patient")
          proc_pat(config, hc, input_dir_file_system, output_dir_file_system)
          println("processing Resources")
          config.resc_types.foreach(resc_type => proc_resc(config, hc, input_dir_file_system, resc_type, output_dir_file_system))
          println("combining Patient")
          combine_pat(config, hc, input_dir_file_system, output_dir_file_system)

        }
      case None =>
    }


  spark.stop()


  }

  private def proc_pat(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) = {
    val count = new AtomicInteger(0)

    val input_file_patient = config.input_dir + "/Patient.json"
    val input_file_patient_path = new Path(input_file_patient)
    val input_file_patient_input_stream = input_dir_file_system.open(input_file_patient_path)

    println("loading " + input_file_patient)

    val obj = Json.parse(input_file_patient_input_stream)
    val entry = (obj \ "entry").get.as[List[JsObject]]
    val n = entry.size

    entry.par.foreach(obj1 => {
      val resource = (obj1 \ "resource").get.as[JsObject]
      val patient_num = (resource \ "id").get.as[Int]

      println("processing " + count.incrementAndGet + " / " + n + " " + patient_num)

      val output_file = config.output_dir + "/Patient/" + patient_num
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {

        val lat = (resource \ "latitude").get.as[Double]
        val lon = (resource \ "longitude").get.as[Double]
        val obj2 = resource ++ Json.obj(
          "lat" -> lat,
          "lon" -> lon
        )

        Utils.writeToFile(hc, output_file, Json.stringify(obj2))
      }

    })
  }

  private def proc_resc(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, resc_type: String, output_dir_file_system: FileSystem) {
    val count = new AtomicInteger(0)

    val input_file_resc = config.input_dir + "/" + resc_type + ".json"
    val input_file_resc_path = new Path(input_file_resc)
    val input_file_resc_input_stream = input_dir_file_system.open(input_file_resc_path)

    println("loading " + input_file_resc)

    val obj = Json.parse(input_file_resc_input_stream)
    val entry = (obj \ "entry").get.as[List[JsObject]]
    val n = entry.size

    entry.par.foreach(obj1 => {

      val id = (obj1 \ "id").get.as[Int]

      println("processing " + count.incrementAndGet + " / " + n + " " + id)

      val patient_num = (obj1 \ "subject" \ "reference").get.as[String].split("/")(1).toInt

      val output_file = config.output_dir + "/" + resc_type + "/" + patient_num + "/" + id
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {
        val obj2 = (obj1 \ "resource").get

        Utils.writeToFile(hc, output_file, Json.stringify(obj2))
      }

    })
  }

  private def combine_pat(config: PreprocFIHRConfig, hc: Configuration, input_dir_file_system: FileSystem, output_dir_file_system: FileSystem) {
    val input_dir = config.output_dir + "/Patient"
    val input_dir_path = new Path(input_dir)

    val count = new AtomicInteger(0)

    val input_file_iter = output_dir_file_system.listFiles(input_dir_path, false)
    while(input_file_iter.hasNext()) {

      val input_file_status = input_file_iter.next()
      val input_file_path = input_file_status.getPath
      val input_file_input_stream = output_dir_file_system.open(input_file_path)
      var obj = Json.parse(input_file_input_stream).as[JsObject]

      val patient_num = input_file_path.getName
      println("processing " + count.incrementAndGet + " " + patient_num)

      config.resc_types.foreach(resc_type => {
        var arr = Json.arr()
        val input_resc_dir = config.output_dir + "/" + resc_type + "/" + patient_num
        val input_resc_dir_path = new Path(input_resc_dir)
        if(output_dir_file_system.exists(input_resc_dir_path)) {
          val input_resc_file_iter = output_dir_file_system.listFiles(input_resc_dir_path, false)
          while(input_resc_file_iter.hasNext()) {
            val input_resc_file_status = input_resc_file_iter.next()
            val input_resc_file_path = input_resc_file_status.getPath
            val input_resc_file_input_stream = output_dir_file_system.open(input_resc_file_path)
            val obj_resc = Json.parse(input_resc_file_input_stream)
            arr +:= obj_resc
          }
          obj ++= Json.obj(
            resc_type -> arr
          )
        }
      })

      val output_file = config.output_dir + "/" + patient_num
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {
        Utils.writeToFile(hc, output_file, Json.stringify(obj))
      }

    }

  }

}
