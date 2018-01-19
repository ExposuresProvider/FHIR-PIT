// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Preproc {

  def time[R](block: =>R) : R = {
    val start = System.nanoTime
    val res = block
    val finish = System.nanoTime
    val duration = (finish - start) / 1e9d
    println("time " + duration + "s")
    res
  }

  def meta(keyvals:Seq[String], cols:Seq[String]) : Seq[String] = for {x <- keyvals; y <- cols} yield x + "_" + y

  def longToWide(df: DataFrame, keycol : String) : DataFrame = {
    val keyvals = df.select(keycol).distinct.rdd.map(r => r.getString(0)).collect.toSeq

    val cols = Seq(
          "valueflag_cd",
          "valtype_cd",
          "nval_num",
          "tval_char",
          "units_cd",
          "start_date",
          "end_date"
        )

    println(keyvals.length + " columns")
    time {
      val pivot = new Pivot(
        keycol,
        keyvals,
        cols
      )

      df.groupBy("patient_num", "encounter_num").agg(pivot(
        cols.map(x => df.col(x)) : _*
      )).as("sparse")

    } 

  }

  def main(args: Array[String]) {

    // val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()
    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val ofif = args(0)

    val df = spark.read.format("csv").option("header", true).load(ofif)

    df.createGlobalTempView("observation_fact")

    // mdctn
    println("processing mdctn")
    val mdctn = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', modifier_cd, '_', instance_num) concept_cd_modifier_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date" +
      " from global_temp.observation_fact where concept_cd like 'MDCTN:%'")

    //mdctn.persist(StorageLevel.MEMORY_AND_DISK);

    val mdctn_wide = longToWide(mdctn, "concept_cd_modifier_cd_instance_num")

    mdctn_wide.createGlobalTempView("mdctn_wide")

    // icd
    val icd = spark.sql("select patient_num, encounter_num, concept_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

    val icd_wide = longToWide(icd, "concept_cd")

    icd_wide.createGlobalTempView("icd_wide")

    // loinc
    val loinc = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

    val loinc_wide = longToWide(loinc, "concept_cd_instance_num")

    loinc_wide.createGlobalTempView("loinc_wide")

    // vital
    val vital = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'VITAL:%'")

    val vital_wide = longToWide(vital, "concept_cd_instance_num")
      
    vital_wide.createGlobalTempView("vital_wide")
    spark.stop()
  }
}
