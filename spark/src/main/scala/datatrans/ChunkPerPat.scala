package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import datatrans.Utils._
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object ChunkPerPat {

  def writePartitionsCSV(spark : SparkSession, pdif : String, pdod : String) = {
    println("loading from " + pdif)
    val pddf = spark.read.format("csv").option("header", true).load(pdif)

    println("writing to " + pdod)
    pddf.write.partitionBy("patient_num").option("header", false).csv(pdod)

    val hc = spark.sparkContext.hadoopConfiguration
    val pddpath = new Path(pdod)
    val dfs = pddpath.getFileSystem(hc)
    for (dir <- dfs.listStatus(pddpath)) {
      if(dir.isDirectory) {
        val dpath = dir.getPath
        val fpath = new Path(dpath + ".csv")
        val ffs = fpath.getFileSystem(hc)

        println("copy to " + fpath)
        FileUtil.copyMerge(dfs, dpath, ffs, fpath, true, hc, null)
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
