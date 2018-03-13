package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time._
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object PreprocPerPatSeriesToVectorHeader {


  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val ofif = args(0)
      val col = args(1)
      val output_file = args(2)

      val df = spark.read.format("csv").option("header", true).load(ofif)
      val odf = df.select(col).distinct.map(r => r.getString(0))

      odf.write.csv(output_file)

      spark.stop()

    }
  }
}
