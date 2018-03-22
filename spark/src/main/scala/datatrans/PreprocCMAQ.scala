package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import datatrans.Utils.{CSV, JSON, time, typeToTrans}
import org.apache.hadoop.fs.{FileUtil, LocatedFileStatus, Path}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class PreprocCMAQ {
  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val input_file = args(0)
      val output_dir = args(1)

      val df = spark.read.csv(input_file)
      df.write.partitionBy("row", "col").csv(output_dir)

      val hc = spark.sparkContext.hadoopConfiguration
      val output_dir_path = new Path(output_dir)
      val output_dir_fs = output_dir_path.getFileSystem(hc)

      val files = output_dir_fs.listFiles(output_dir_path, false)

      while(files.hasNext) {
        val file = files.next
        val filename = file.getPath.getName
      }

      spark.stop()

    }
  }
}
