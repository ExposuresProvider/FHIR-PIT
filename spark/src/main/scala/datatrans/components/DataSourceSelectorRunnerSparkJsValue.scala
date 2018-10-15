package datatrans.components

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json.{JsValue, Json}

object DataSourceSelectorRunnerSparkJsValue {

  def run(spark: SparkSession, patient_dimension : String, patient_num : String, sequential : Boolean, input_format: String, dataSourceSelect : DataSourceSelectorFormatter[SparkSession,(String, JsValue)], output_format: String): Unit = {

    import spark.implicits._

    println("loading patient_dimension from " + patient_dimension)
    val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)

    val patl0 = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList

    val patl = if (sequential) patl0 else patl0.par

    val hc = spark.sparkContext.hadoopConfiguration

    val count = new AtomicInteger(0)
    val n = patl.size

    patl.foreach {
      case (r, lat, lon) =>
        println("processing " + count.incrementAndGet + " / " + n + " " + r)

        val jsvalue = Json.obj(
          "lat" -> lat,
          "lon" -> lon
        )

        dataSourceSelect.getOutput(spark, (r, jsvalue)).foreach {
          case (i2, str) =>
            val output_file = output_format.replace("%i", i2)
            val output_file_path = new Path (output_file)
            val output_file_file_system = output_file_path.getFileSystem (hc)

            if(output_file_file_system.exists(output_file_path)) {
              println(output_file + " exists")
            } else {
              writeToFile(hc, output_file, str())
            }
        }

    }

    spark.stop()


  }
}
