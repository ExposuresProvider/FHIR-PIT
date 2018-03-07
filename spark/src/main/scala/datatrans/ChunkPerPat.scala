package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import datatrans.Utils._
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils


object ChunkPerPat {

  def writePartitionsCSV(spark : SparkSession, pdif : String, output_directory : String) = {
    println("loading from " + pdif)
    val patient_dimension_dataframe = spark.read.format("csv").option("header", true).load(pdif)

    val cols = for(col <- patient_dimension_dataframe.columns if col != "patient_num") yield col

    println("writing to " + output_directory)
    patient_dimension_dataframe.write.partitionBy("patient_num").option("header", false).csv(output_directory)

    val hc = spark.sparkContext.hadoopConfiguration
    val output_directory_path = new Path(output_directory)
    val output_directory_file_system = output_directory_path.getFileSystem(hc)

    val headers = cols.mkString(",") + "\n"

    for (dir <- output_directory_file_system.listStatus(output_directory_path)) {
      if(dir.isDirectory) {
        val dir_path = dir.getPath
        val no_header_csv_path = new Path(dir_path + ".csv_noheader")
        val csv_path = new Path(dir_path + ".csv")
        val output_file_file_system = no_header_csv_path.getFileSystem(hc)

        println("copy to " + no_header_csv_path)
        FileUtil.copyMerge(output_directory_file_system, dir_path, output_file_file_system, no_header_csv_path, true, hc, null)
        println("copy to " + csv_path)

        val csv_output_stream = output_file_file_system.create(csv_path)
        val csv_no_header_input_stream = output_file_file_system.open(no_header_csv_path)
        val csv_no_header = IOUtils.toString(csv_no_header_input_stream, "UTF-8")
        csv_no_header_input_stream.close()

        csv_output_stream.write(headers.getBytes("utf-8"))
        csv_output_stream.write(csv_no_header.getBytes("utf-8"))
        csv_output_stream.close()
        output_file_file_system.delete(no_header_csv_path, false)

      }

    }


  }

  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val pdif = args(0)
      val vdif = args(1)
      val ofif = args(2)
      val output_path = args(3)

      val pdod = output_path + "/patient_dimension"
      writePartitionsCSV(spark, pdif, pdod)

      val vdod = output_path + "/visit_dimension"
      writePartitionsCSV(spark, vdif, vdod)

      val ofod = output_path + "/observation_fact"
      writePartitionsCSV(spark, ofif, ofod)

      spark.stop()

    }
  }
}
