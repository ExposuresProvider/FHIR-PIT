package datatrans

import java.io._
import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ SparkSession, Column, Row }
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.{ ListBuffer, MultiMap }
import scopt._
import datatrans._
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

case class CSVTableConfig(
  input_files : Seq[String] = Seq(),
  output_file : String = "",
  start_date : DateTime = new DateTime(0),
  end_date : DateTime = new DateTime(0)
)

object PreprocCSVTable {
  
  def main(args: Array[String]) {
    val parser = new OptionParser[CSVTableConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_files").required.action((x,c) => c.copy(input_files = x.split(",")))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("end_date").action((x,c) => c.copy(end_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

//    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, CSVTableConfig()) match {
      case Some(config) =>

        time {
          val hc = spark.sparkContext.hadoopConfiguration
          val start_date_joda = config.start_date
          val end_date_joda = config.end_date

          val dfs = config.input_files.map(input_file => {
            spark.read.format("csv").option("header", value = true).load(input_file)
          })

          val df = dfs.reduce(_.join(_, "patient_num")).drop("patient_num", "encounter_num")

          writeDataframe(hc, config.output_file, df)


        }
      case None =>
    }


    spark.stop()


  }
