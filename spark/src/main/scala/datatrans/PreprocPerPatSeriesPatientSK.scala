package datatrans

import datatrans.Utils._
import org.apache.spark.sql.{Row, SparkSession}
import scopt._
import play.api.libs.json._

case class PreprocPerPatSeriesPatientSKConfig(
                   patient_dimension : String = "",
                   input_directory : String = "",
                   output_prefix : String = ""
                 )

object PreprocPerPatSeries {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesPatientSKConfig]("series_to_vector") {
      head("series")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = x))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = x))
    }

    parser.parse(args, PreprocPerPatSeriesPatientSKConfig()) match {
      case Some(config) =>
        time {
          val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

          spark.sparkContext.setLogLevel("WARN")

          // For implicit conversions like converting RDDs to DataFrames
          import spark.implicits._

          val hc = spark.sparkContext.hadoopConfiguration

          val pdif = config.patient_dimension
          println("loading patient_dimension from " + pdif)
          val pddf0 = spark.read.format("csv").option("header", value = true).option("delimiter", "|").load(pdif)

          pddf0.select(pddf0("PATIENTSK_CHANGEME"), pddf0("XCOORDS2017_DROPME"), pddf0("YCOORDS2017_DROPME")).foreach((row : Row) => {
            val patient_num = row.getString(0)
            val obj = Json.obj(
              "lon" -> row.getDouble(1),
              "lat" -> row.getDouble(2)
            )
            val json = obj.toString()
            val output_filename = config.output_prefix + patient_num
            writeToFile(hc, output_filename, json)
          })



          spark.stop()
        }

      case None =>
    }
  }
}
