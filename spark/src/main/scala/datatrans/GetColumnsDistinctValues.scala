package datatrans

import datatrans.Utils._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scopt._

// cut -f3 -d, observation_fact.csv | tail -n +2 | tr -d '"' | sed '/^\s*$/d' | sort -u > json/header0
// cut -f6 -d, visit_dimension.csv | tail -n +2 | tr -d '"' | sed '/^\s*$/d' | sort -u > json/header1

case class PreprocPerPatSeriesToHeaderConfig(
                   tables : Seq[String] = Seq(),
                   columns : Seq[String] = Seq(),
                   output_file : String = ""
                 )

object GetColumnsDistinctValues {


  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesToHeaderConfig]("series_to_vector") {
      head("series_to_vector")
      opt[Seq[String]]("tables").required.action((x,c) => c.copy(tables = x))
      opt[Seq[String]]("columns").required.action((x,c) => c.copy(columns = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, PreprocPerPatSeriesToHeaderConfig()) match {
      case Some(config) =>
        time {
          val keys = new ListBuffer[String]()
          config.tables zip config.columns foreach { case (table, column) =>
            println("loading " + table)
            val pddf0 = spark.read.format("csv").option("header", value = true).load(table)

            val patl = pddf0.select(column).distinct.map(r => r.getString(0)).collect

            keys ++= patl

            pddf0.unpersist()

          }
          val hc = spark.sparkContext.hadoopConfiguration

          writeToFile(hc, config.output_file, keys.distinct.mkString("\n"))
        }
      case None =>
    }


    spark.stop()


  }
}
