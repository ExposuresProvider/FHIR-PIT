package datatrans

import java.sql.Date
import java.time.ZoneId

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.collection.JavaConversions._
import org.joda.time._

import scala.collection.mutable.ListBuffer
import scopt._

case class PreprocPerPatSeriesToHeaderConfig(
                   tables : Seq[String] = Seq(),
                   columns : Seq[String] = Seq(),
                   output_file : String = ""
                 )

object PreprocPerPatSeriesToHeader {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesToHeaderConfig]("series_to_vector") {
      head("series_to_vector")
      opt[Seq[String]]("tables").required.action((x,c) => c.copy(tables = x))
      opt[Seq[String]]("columns").required.action((x,c) => c.copy(columns = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesToHeaderConfig()) match {
      case Some(config) =>
        time {
          val keys = new ListBuffer[String]()
          config.tables zip config.columns foreach { case (table, column) =>
            println("loading " + table)
            val pddf0 = spark.read.format("csv").option("header", true).load(table)

            val patl = pddf0.select(column).map(r => r.getString(0)).collect

            keys ++= patl
          }
          val hc = spark.sparkContext.hadoopConfiguration

          writeToFile(hc, config.output_file, keys.mkString(","))
        }
      case None =>
    }


    spark.stop()


  }
}
