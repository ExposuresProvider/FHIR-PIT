package datatrans

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import scopt._
import net.jcazevedo.moultingyaml._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocCombineDataConfig(
                              patient_dimension : String = "",
                              time_series: String = "",
                              input_resc_dir : String = "",
                              output_dir : String = "",
                              resources : Seq[String] = Seq()
                            ) extends StepConfig {
  val configConfig = PreprocCombineData
}

object CombineDataYamlProtocol extends DefaultYamlProtocol {
  implicit val combineDataYamlFormat = yamlFormat5(PreprocCombineDataConfig)
}

object PreprocCombineData extends StepConfigConfig {

  type ConfigType = PreprocCombineDataConfig

  val yamlFormat = CombineDataYamlProtocol.combineDataYamlFormat

  val configType = "CombineData"

  def step(spark: SparkSession, config: PreprocCombineDataConfig) : Unit = {
    Utils.time {
      proc_resc(spark, config)
    }
  }

  private def toJsValue(obj: Any) : JsValue = {
    if (obj == null) {
      JsNull
    } else obj match {
      case i: Int =>
        JsNumber(i)
      case fl: Float =>
        JsNumber(BigDecimal(fl))
      case d: Double =>
        JsNumber(BigDecimal(d))
      case str: String =>
        JsString(str)
      case _ =>
        throw new RuntimeException("cannot convert to JSValue " + obj)
    }
  }

  private def proc_resc(spark: SparkSession, config: PreprocCombineDataConfig) {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val hc = spark.sparkContext.hadoopConfiguration

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

    val resourceMap = TrieMap[String, DataFrame]()

    patl.par.foreach(patient_num =>  {

      println("processing " + count.incrementAndGet + " / " + n + " " + patient_num)

      val input_file = config.time_series + "/" + patient_num
      val input_file_path = new Path(input_file)

      if(!time_series_file_system.exists(input_file_path)) {
        println("json not found, skipped " + patient_num)
      } else {
        val input_file_input_stream = time_series_file_system.open(input_file_path)
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
                  val df2 = spark.read.format("csv").option("header", value = true).load(resource_file)
                  resourceMap(resource) = df2
                  df2
                })

                val columns = df.columns.filter(x => x != "patient_num")
                val row = df.select(df.col("*")).where(df.col("patient_num").equalTo(patient_num)).first
                obj ++= JsObject(columns.map(column => column -> toJsValue(row(row.fieldIndex(column)))))
              } else if (input_resc_dir_file_system.isDirectory(resource_file_path)) {
                val input_resc_file = config.input_resc_dir + "/" + resource + "/" + patient_num
                val input_resc_file_path = new Path(input_resc_file)
                if (input_resc_dir_file_system.exists(input_resc_file_path)) {
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
      }

    })

  }


}
