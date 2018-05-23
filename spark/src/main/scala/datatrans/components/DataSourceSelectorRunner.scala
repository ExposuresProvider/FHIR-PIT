package datatrans.components

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

object DataSourceSelectorRunner {

  def run(patient_dimension : String, patient_num : String, dataSourceSelect : DataSourceSelectorFormatter, input_format: String, output_file: String): Unit = {
    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    println("loading patient_dimension from " + patient_dimension)
    val hc = spark.sparkContext.hadoopConfiguration
    val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
    val patl = pddf0.select(patient_num).flatMap(r => {
      val i = r.getString(0)
      val input_file = input_format.replace("%i", i)
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)
      if(!input_file_file_system.exists(input_file_path)) {
        println("json not found, skipped " + i)
        Seq()
      } else {
        println("loading json from " + input_file)
        val input_file_input_stream = input_file_file_system.open(input_file_path)

        val jsvalue = Json.parse(input_file_input_stream)
        input_file_input_stream.close()

        dataSourceSelect.getRows(jsvalue)
      }

    }).collect.map(r => r.mkString(",")).mkString("\n")

    val header = dataSourceSelect.header.mkString(",") + "\n"

    writeToFile(hc, output_file, header + patl)

    spark.stop()


  }
}
