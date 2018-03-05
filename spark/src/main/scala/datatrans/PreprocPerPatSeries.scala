package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import datatrans.Utils._
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object PreprocPerPatSeries {


  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val form = args(0) match {
        case "csv" => CSV
        case "json" => JSON
      }
      val pdif = args(1)
      val vdif = args(2)
      val ofif = args(3)
      val output_path = args(4)

      println("loading patient_dimension from " + pdif)
      val pddf = spark.read.format("csv").option("header", true).load(pdif)
      println("loading visit_dimension from " + vdif)
      val vddf = spark.read.format("csv").option("header", true).load(vdif)
      println("loading observation_fact from " + ofif)
      val ofdf = spark.read.format("csv").option("header", true).load(ofif)

      val patl = pddf.select("patient_num").map(r => r.getString(0)).collect.toList

      for(p <- patl) {
        println("processing pid " + p)
        val inout = vddf.filter($"patient_num" === p).select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

        val pat = pddf.filter($"patient_num" === p).select("patient_num", "race_cd", "sex_cd", "birth_date")

        val lat = ofdf.filter($"patient_num" === p).filter($"concept_cd".like("GEO:LAT")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lat"))

        val lon = ofdf.filter($"patient_num" === p).filter($"concept_cd".like("GEO:LONG")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lon"))

        val features = pat
          .join(inout, "patient_num")
          .join(lat, "patient_num")
          .join(lon, "patient_num")

        features.persist(StorageLevel.MEMORY_AND_DISK);

        // observation
        val observation_wide = time {
          val cols = Seq(
            "valueflag_cd",
            "valtype_cd",
            "nval_num",
            "tval_char",
            "units_cd",
            "start_date",
            "end_date"
          )
          val observation = ofdf.filter($"patient_num" === p).select("patient_num", "encounter_num", "concept_cd", "instance_num", "modifier_cd", "valueflag_cd", "valtype_cd", "nval_num", "tval_char", "units_cd", "start_date", "end_date")

          val observation_wide = longToWide(observation, Seq("patient_num"), Seq("encounter_num", "concept_cd", "instance_num", "modifier_cd"), cols, "observation")

          observation_wide.persist(StorageLevel.MEMORY_AND_DISK)

          observation_wide
        }

        // visit
        val visit_wide = time {
          val cols = Seq(
            "inout_cd",
            "start_date",
            "end_date"
          )
          val visit = vddf.filter($"patient_num" === p).select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

          val visit_wide = longToWide(visit, Seq("patient_num"), Seq("encounter_num"), cols, "visit")

          visit_wide.persist(StorageLevel.MEMORY_AND_DISK)

          visit_wide
        }

        //      val merge_map = udf((a:Map[String,Any], b:Map[String,Any]) => mergeMap(a,b))

        val features_wide = features
          .join(observation_wide, Seq("patient_num"))
          .join(visit_wide, Seq("patient_num"))
        //        .select($"patient_num", $"encounter_num", $"sex_cd", $"race_cd", $"birth_date", merge_map($"observation", $"visit"))

        writeCSV(spark, features_wide, output_path + p,form)

      }

      spark.stop()

    }
  }
}
