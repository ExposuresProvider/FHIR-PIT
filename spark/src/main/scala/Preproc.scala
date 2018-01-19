// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


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

  def longToWide(df: DataFrame, keycol : String, cols : Seq[String], col:String) : DataFrame = {
    val keyvals = df.select(keycol).distinct.rdd.map(r => r.getString(0)).collect.toSeq


    println(keyvals.length + " columns")
    time {
      val pivot = new Pivot(
        keycol,
        keyvals,
        cols
      )

      df.groupBy("patient_num", "encounter_num").agg(pivot(
        cols.map(x => df.col(x)) : _*
      )).as(col)

    } 

  }

  def main(args: Array[String]) {

    // val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()
    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val pdif = args(0)
    val vdif = args(1)
    val ofif = args(2)

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
    println("processing mdctn")
    val mdctn = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', modifier_cd, '_', instance_num) concept_cd_modifier_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date" +
      " from global_temp.observation_fact where concept_cd like 'MDCTN:%'")

    //mdctn.persist(StorageLevel.MEMORY_AND_DISK);

    val mdctn_wide = longToWide(mdctn, "concept_cd_modifier_cd_instance_num", cols, "mdctn")

    mdctn_wide.createGlobalTempView("mdctn_wide")

    // icd
    val icd = spark.sql("select patient_num, encounter_num, concept_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

    val icd_wide = longToWide(icd, "concept_cd", Seq("start_date", "end_date"), "icd")

    icd_wide.createGlobalTempView("icd_wide")

    // loinc
    val loinc = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

    val loinc_wide = longToWide(loinc, "concept_cd_instance_num", cols, "loinc")

    loinc_wide.createGlobalTempView("loinc_wide")

    // vital
    val vital = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'VITAL:%'")

    val vital_wide = longToWide(vital, "concept_cd_instance_num", cols, "vital")

    vital_wide.createGlobalTempView("vital_wide")

    val inout = vddf.select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

    val pat = pddf.select("patient_num", "race_cd", "sex_cd", "birth_date")

    val lat = ofdf.filter($"concept_cd".like("GEO:LAT")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num"))

    val lon = ofdf.filter($"concept_cd".like("GEO:LON")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num"))

    spark.stop()
  }
}
