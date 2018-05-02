package datatrans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.commons.lang3.StringEscapeUtils
import datatrans.Utils._
import java.lang.RuntimeException

object Preproc {

  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val form = args(0) match {
        case "csv" => CSV("!")
        case "json" => JSON
      }
      val pdif = args(1)
      val vdif = args(2)
      val ofif = args(3)

      val pddf = spark.read.format("csv").option("header", true).load(pdif)
      val vddf = spark.read.format("csv").option("header", true).load(vdif)
      val ofdf = spark.read.format("csv").option("header", true).load(ofif)

      pddf.createGlobalTempView("patient_dimension")
      vddf.createGlobalTempView("visit_dimension")
      ofdf.createGlobalTempView("observation_fact")

      val cols = Seq(
        "valueflag_cd",
        "valtype_cd",
        "nval_num",
        "tval_char",
        "units_cd",
        "start_date",
        "end_date"
      )

      // mdctn
      val mdctn_wide = time {
        val mdctn = spark.sql("select patient_num, encounter_num, concept_cd, instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date" +
          " from global_temp.observation_fact where concept_cd like 'MDCTN:%'")

        val mdctn_wide = groupByAndAggregate(mdctn, Seq("patient_num", "encounter_num"), Seq("concept_cd", "instance_num", "modifier_cd"), cols, "mdctn")

        mdctn_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, mdctn_wide, "/tmp/mdctn_wide",form)

        mdctn_wide
      }

      // icd
      val icd_wide = time {
        val icd = spark.sql("select patient_num, encounter_num, concept_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

        val icd_wide = groupByAndAggregate(icd, Seq("patient_num", "encounter_num"), Seq("concept_cd"), Seq("start_date", "end_date"), "icd")

        icd_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, icd_wide, "/tmp/icd_wide",form)

        icd_wide
      }

      // loinc
      val loinc_wide = time {
        val loinc = spark.sql("select patient_num, encounter_num, concept_cd, instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

        val loinc_wide = groupByAndAggregate(loinc, Seq("patient_num", "encounter_num"), Seq("concept_cd", "instance_num"), cols, "loinc")

        loinc_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, loinc_wide, "/tmp/loinc_wide",form)

        loinc_wide
      }

      // vital
      val vital_wide = time {
        val vital = spark.sql("select patient_num, encounter_num, concept_cd, instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'VITAL:%'")

        val vital_wide = groupByAndAggregate(vital, Seq("patient_num", "encounter_num"), Seq("concept_cd", "instance_num"), cols, "vital")

        vital_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, vital_wide, "/tmp/vital_wide",form)

        vital_wide
      }

      val inout = vddf.select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

      val pat = pddf.select("patient_num", "race_cd", "sex_cd", "birth_date")

      val lat = ofdf.filter($"concept_cd".like("GEO:LAT")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lat"))

      val lon = ofdf.filter($"concept_cd".like("GEO:LONG")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lon"))

      val features = pat
        .join(inout, "patient_num")
        .join(lat, "patient_num")
        .join(lon, "patient_num")

      features.persist(StorageLevel.MEMORY_AND_DISK);

//      writeCSV(spark, features, "/tmp/features",form)

      val features_wide = features
        .join(icd_wide, Seq("patient_num", "encounter_num"))
        .join(loinc_wide, Seq("patient_num", "encounter_num"))
        .join(mdctn_wide, Seq("patient_num", "encounter_num"))
        .join(vital_wide, Seq("patient_num", "encounter_num"))

      writeCSV(spark, features_wide, "/tmp/features_wide",form)

      spark.stop()
    }
  }
}
