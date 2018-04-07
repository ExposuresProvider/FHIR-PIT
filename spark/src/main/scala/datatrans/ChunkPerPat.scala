package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession


object ChunkPerPat {

  def writePartitionsCSV(spark : SparkSession, pdif : String, output_directory : String): Unit = {
    println("loading from " + pdif)
    val patient_dimension_dataframe = spark.read.format("csv").option("header", value = true).load(pdif)

    val cols = for(col <- patient_dimension_dataframe.columns if col != "patient_num") yield col

    println("writing to " + output_directory)
    patient_dimension_dataframe.write.partitionBy("patient_num").option("header", value = false).csv(output_directory)

    val hc = spark.sparkContext.hadoopConfiguration
    val output_directory_path = new Path(output_directory)
    val output_directory_file_system = output_directory_path.getFileSystem(hc)

    val headers = cols.mkString(",") + "\n"
    val headers_filename = f"$output_directory/.header"
    writeToFile(hc, headers_filename, headers)
    val header_path = new Path(headers_filename)

    for (dir <- output_directory_file_system.listStatus(output_directory_path)) {
      if(dir.isDirectory) {
        val dir_path = dir.getPath
        val csv_filename = dir_path + ".csv"

        println("copy to " + csv_filename)
        copyMerge(hc, output_directory_file_system, overwrite = true, csv_filename, header_path, dir_path)

      }
    }
    output_directory_file_system.delete(header_path, false)


  }

  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      writePartitionsCSV(spark, args(0), args(1))

      spark.stop()

    }
  }
}
