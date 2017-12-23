// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "4g").config("spark.driver.memory", "16g").getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

val df = spark.read.format("csv").option("header", true).load("/tmp/observation_fact.csv")

df.createGlobalTempView("observation_fact")

// icd
val icd = spark.sql("select * from global_temp.observation_fact where concept_cd like 'ICD%'")

val icd_wide = icd.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("start_date"), collect_list("end_date"))

icd_wide.createGlobalTempView("icd_wide")

// loinc
val loinc = spark.sql("select * from global_temp.observation_fact where concept_cd like 'LOINC:%'")

val loinc_wide = loinc.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("valtype_cd"), collect_list("nval_num"),
  collect_list("tval_char"), collect_list("units_cd"), collect_list("start_date"), collect_list("end_date"))

loinc_wide.createGlobalTempView("loinc_wide")

// mdctn
val mdctn = spark.sql("select * from global_temp.observation_fact where concept_cd like 'MDCTN:%'")

val mdctn_wide = mdctn.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("modifier_cd"), collect_list("valueflag_cd"), collect_list("valtype_cd"), collect_list("nval_num"),
  collect_list("tval_char"), collect_list("units_cd"), collect_list("start_date"), collect_list("end_date"))

mdctn_wide.createGlobalTempView("mdctn_wide")

// vital
val vital = spark.sql("select * from global_temp.observation_fact where concept_cd like 'VITAL:%'")

val vital_wide = vital.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("modifier_cd"), collect_list("valueflag_cd"), collect_list("valtype_cd"), collect_list("nval_num"),
  collect_list("tval_char"), collect_list("units_cd"), collect_list("start_date"), collect_list("end_date"))

vital_wide.createGlobalTempView("vital_wide")
