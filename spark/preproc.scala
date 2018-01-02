// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer

val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

val df = spark.read.format("csv").option("header", true).load("/tmp/observation_fact.csv")

df.createGlobalTempView("observation_fact")

// mdctn
val mdctn = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date" +
  " from global_temp.observation_fact where concept_cd like 'MDCTN:%'")
//mdctn.persist(StorageLevel.MEMORY_AND_DISK);

val cols = mdctn.select("concept_cd_instance_num").distinct.map(r => r.getString(0)).collect.toSeq
val step = 1000
val mdctn_wide_tables = new ListBuffer[DataFrame]()
println(cols.length + " columns")
for (i <- 0 until cols.length by step) {
  println("processing columns " + i + " to " + Math.min(i+step, cols.length))
  val mdctn_wide_table = mdctn.groupBy("patient_num", "encounter_num").pivot("concept_cd_instance_num", cols.slice(i, Math.min(i+step, cols.length))).agg(first("modifier_cd"),
    first("valueflag_cd"),
    first("valtype_cd"),
    first("nval_num"),
    first("tval_char"),
    first("units_cd"),
    first("start_date"),
    first("end_date")).sort($"patient_num".asc, $"encounter_num".asc)
  mdctn_wide_tables += mdctn_wide_table
  mdctn_wide_table.write.format("csv").save("/tmp/mdctn_wide_table" + i + ".csv")
}

// icd
val icd = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

val icd_wide = icd.groupBy("patient_num", "encounter_num").pivot("concept_cd_instance_num").agg(first("start_date"), first("end_date"))

icd_wide.createGlobalTempView("icd_wide")

// loinc
val loinc = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

val loinc_wide = loinc.groupBy("patient_num", "encounter_num").pivot("concept_cd_instance_num").agg(first("valtype_cd"), first("nval_num"),
  first("tval_char"), first("units_cd"), first("start_date"), first("end_date"))

loinc_wide.createGlobalTempView("loinc_wide")




//mdctn_wide_modifier_cd.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_valueflag_cd.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_valtype_cd.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_nval_num.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_tval_char.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_units_cd.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_start_date.persist(StorageLevel.MEMORY_AND_DISK);
//mdctn_wide_end_date.persist(StorageLevel.MEMORY_AND_DISK);

val mdctn_wide2 = mdctn_wide_modifier_cd.join(mdctn_wide_valueflag_cd, Seq("patient_num", "encounter_num"))
mdctn_wide2.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide3 = mdctn_wide2.join(mdctn_wide_valtype_cd, Seq("patient_num", "encounter_num"))
mdctn_wide3.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide4 = mdctn_wide3.join(mdctn_wide_nval_num, Seq("patient_num", "encounter_num"))
mdctn_wide4.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide5 = mdctn_wide4.join(mdctn_wide_tval_char, Seq("patient_num", "encounter_num"))
mdctn_wide5.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide6 = mdctn_wide5.join(mdctn_wide_units_cd, Seq("patient_num", "encounter_num"))
mdctn_wide6.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide7 = mdctn_wide6.join(mdctn_wide_start_date, Seq("patient_num", "encounter_num"))
mdctn_wide7.persist(StorageLevel.MEMORY_AND_DISK);
val mdctn_wide = mdctn_wide7.join(mdctn_wide_end_date, Seq("patient_num", "encounter_num"))
mdctn_wide.persist(StorageLevel.MEMORY_AND_DISK);

mdctn_wide.createGlobalTempView("mdctn_wide")

// vital
val vital = spark.sql("select * from global_temp.observation_fact where concept_cd like 'VITAL:%'")

val vital_wide = vital.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("modifier_cd"), collect_list("valueflag_cd"), collect_list("valtype_cd"), collect_list("nval_num"),
  collect_list("tval_char"), collect_list("units_cd"), collect_list("start_date"), collect_list("end_date"))

vital_wide.createGlobalTempView("vital_wide")
