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

// cut -f3 -d, observation_fact.csv | tail -n +2 | tr -d '"' | sed '/^\s*$/d' | sort -u > json/header0
// cut -f6 -d, visit_dimension.csv | tail -n +2 | tr -d '"' | sed '/^\s*$/d' | sort -u > json/header1

case class PreprocPerPatSeriesToPatNumConfig(
                   table : String = "",
                   column : String = "",
                   output_file : String = ""
                 )

object GetColumnDistinctValues {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesToPatNumConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("table").required.action((x,c) => c.copy(table = x))
      opt[String]("column").required.action((x,c) => c.copy(column = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesToPatNumConfig()) match {
      case Some(config) =>
        time {
          println("loading " + config.table)
          val pddf0 = spark.read.format("csv").option("header", true).load(config.table)

          val patl = pddf0.select(config.column).distinct.map(r => r.getString(0)).collect

          val hc = spark.sparkContext.hadoopConfiguration

          writeToFile(hc, config.output_file, patl.mkString("\n"))
        }
      case None =>
    }


    spark.stop()


  }
}
