package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.{FileUtil, Path, PathFilter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.joda.time._
import scopt._

import scala.util.matching.Regex

case class PreprocDailyEnvDataConfig(
                   input_directory : String = "",
                   output_prefix : String = ""
                 )

object PreprocDailyEnvData {
  val schema = StructType(
    Seq(
      StructField("start_date", TimestampType, nullable = true),
      StructField("o3", DoubleType, nullable = true),
      StructField("pm25", DoubleType, nullable = true)
    ))

  def preproceEnvData(config : PreprocDailyEnvDataConfig, spark: SparkSession, filename : String) : Unit =
    time {


      val hc = spark.sparkContext.hadoopConfiguration
      val name = new Path(filename).getName.split("[.]")(0)
      val output_dir = f"${config.output_prefix}${name}Daily"
      val output_dir_path = new Path(output_dir)
      val output_dir_fs = output_dir_path.getFileSystem(hc)

      if (output_dir_fs.exists(output_dir_path)) {
        output_dir_fs.delete(output_dir_path, true)
      }

      val output_filename = f"${config.output_prefix}${name}Daily.csv"
      val output_file_path = new Path(output_filename)
      val output_file_fs = output_file_path.getFileSystem(hc)
      if(!output_file_fs.exists(output_file_path)) {
        val df = spark.read.format("csv").option("header", value = true).schema(schema).load(filename)

        val aggregate = df.withColumn("start_date", to_date(df("a"))).groupBy("start_date").agg(
          avg("o3").alias("o3_avg"),
          avg("pm25").alias("pm25_avg"),
          max("o3").alias("o3_max"),
          max("pm25").alias("pm25_max"),
          min("o3").alias("o3_min"),
          min("pm25").alias("pm25_min"),
          stddev("o3").alias("o3_stddev"),
          stddev("pm25").alias("pm25_stddev")
        )

        aggregate.write.csv(output_dir)

        val header = aggregate.columns.mkString(",") + "\n"
        val header_filename = f"$output_dir/../.header"

        writeToFile(hc, header_filename, header)

        val header_path = new Path(header_filename)

        copyMerge(hc, output_dir_fs, overwrite = true, output_filename, header_path, output_dir_path)

        output_dir_fs.delete(header_path, false)

      } else
        println(output_filename + " exists")

    }



  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocDailyEnvDataConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_directory").required.action((x, c) => c.copy(input_directory = x))
      opt[String]("output_prefix").required.action((x, c) => c.copy(output_prefix = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    parser.parse(args, PreprocDailyEnvDataConfig()) match {
      case Some(config) =>

        time {
          val hc = spark.sparkContext.hadoopConfiguration
          val input_dir_path = new Path(config.input_directory)
          val input_dir_fs = input_dir_path.getFileSystem(hc)

          val itr = input_dir_fs.listStatus(input_dir_path, new PathFilter {
            override def accept(path : Path) : Boolean = path.getName.matches(raw"C\d*R\d*.csv")
          }).par

          val count = new AtomicInteger(0)
          val n = itr.size
          for (file <- itr) {
            println("processing " + count.incrementAndGet + " / " + n + " " + file.getPath)
            preproceEnvData(config, spark, file.getPath.toString)
          }
        }
      case None =>
    }





    spark.stop()


  }
}
