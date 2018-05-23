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

    val patl0 = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList

    val patl = if (sequential) patl0 else patl0.par

    val hc = spark.sparkContext.hadoopConfiguration

    val count = new AtomicInteger(0)
    val n = patl.size

    patl.foreach(r => {
      println("processing " + count.incrementAndGet + " / " + n + " " + r)
      val input_file = input_format.replace("%i", r)
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)
      if(!input_file_file_system.exists(input_file_path)) {
        println("json not found, skipped " + r)
      } else {
        println("loading json from " + input_file)
        val input_file_input_stream = input_file_file_system.open(input_file_path)

        val jsvalue = Json.parse(input_file_input_stream)
        input_file_input_stream.close()

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

    })

    spark.stop()


  }
}
