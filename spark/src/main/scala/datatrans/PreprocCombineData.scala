package datatrans

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.Map
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import scopt._

case class PreprocCombineDataConfig(
                              patient_dimension : String = "",
                              time_series: String = "",
                              input_resc_dir : String = "",
                              output_dir : String = "",
                              resources : Seq[String] = Seq()
                            )

object PreprocCombineData {

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocCombineDataConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").required.action((x,c) => c.copy(patient_dimension = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("input_resc_dir").required.action((x,c) => c.copy(input_resc_dir = x))
      opt[String]("output_dir").required.action((x,c) => c.copy(output_dir = x))
      opt[Seq[String]]("resc_types").required.action((x,c) => c.copy(resources = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    parser.parse(args, PreprocCombineDataConfig()) match {
      case Some(config) =>

        Utils.time {

          proc_resc(spark, config)

        }
      case None =>
    }

    spark.stop()

  }


  private def toJsValue(obj: Any) : JsValue = {
    if(obj.isInstanceOf[Int]) {
      JsNumber(obj.asInstanceOf[Int])
    } else if(obj.isInstanceOf[Float]) {
      JsNumber(BigDecimal(obj.asInstanceOf[Float]))
    } else if(obj.isInstanceOf[Double]) {
      JsNumber(BigDecimal(obj.asInstanceOf[Double]))
    } else if(obj.isInstanceOf[String]) {
      JsString(obj.asInstanceOf[String])
    } else {
      throw new RuntimeException("cannot convert to JSValue " + obj)
    }
  }

  private def proc_resc(spark: SparkSession, config: PreprocCombineDataConfig) {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val hc = spark.sparkContext.hadoopConfiguration
    val input_dir_path = new Path(config.patient_dimension)
    val input_dir_file_system = input_dir_path.getFileSystem(hc)

    val time_series_path = new Path(config.time_series)
    val time_series_file_system = time_series_path.getFileSystem(hc)

    val input_resc_dir_path = new Path(config.input_resc_dir)
    val input_resc_dir_file_system = input_resc_dir_path.getFileSystem(hc)

    val output_dir_path = new Path(config.output_dir)
    val output_dir_file_system = output_dir_path.getFileSystem(hc)

    println("loading patient_dimension from " + config.patient_dimension)
    val pddf0 = spark.read.format("csv").option("header", value = true).load(config.patient_dimension)
    val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList

    val count = new AtomicInteger(0)
    val n = patl.size

    val resourceMap = Map[String, DataFrame]()

    patl.foreach(patient_num =>  {

      println("processing " + count.incrementAndGet + " / " + n + " " + patient_num)

      val input_file = config.time_series + "/" + patient_num
      val input_file_path = new Path(input_file)
      val input_file_input_stream = input_dir_file_system.open(input_file_path)
      var obj = Json.parse(input_file_input_stream).as[JsObject]


      val output_file = config.output_dir + "/" + patient_num
      val output_file_path = new Path(output_file)
      if (output_dir_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {
        config.resources.foreach(
          resource => {
            val resource_file = config.input_resc_dir + "/" + resource
            val resource_file_path = new Path(resource_file)
            if (input_resc_dir_file_system.isFile(resource_file_path)) {
              val df = resourceMap.getOrElse(resource, {
                val df2 = spark.read.format("csv").option("header", value = true).load()
                resourceMap(resource) = df2
                df2
              })

              val columns = df.columns.filter(x => x != "patient_num")
              val row = df.select(df.col("*")).where(df.col("patient_num").equalTo(patient_num)).first
              obj ++= JsObject(columns.map(column => column -> toJsValue(row(row.fieldIndex(column)))))
            } else if (input_resc_dir_file_system.isDirectory(resource_file_path)) {
              var arr = Json.arr()
              val input_resc_file = config.input_resc_dir + "/" + resource + "/" + patient_num
              val input_resc_file_path = new Path(input_resc_file)
              if(input_resc_dir_file_system.exists(input_resc_file_path)) {
                val df = spark.read.format("csv").option("header", value = true).load(input_resc_file)
                val columns = df.columns.filter(x => x != "patient_num")
                val rows = df.collect()
                val arr = JsArray(rows.map(row =>
                  JsObject(columns.map(column => column -> toJsValue(row(row.fieldIndex(column)))))))

                obj ++= Json.obj(
                  resource -> arr
                )
              }
            } else {
              println("unknown object type")
            }
          }
        )

        Utils.writeToFile(hc, output_file, Json.stringify(obj))
      }

    })

  }


}
