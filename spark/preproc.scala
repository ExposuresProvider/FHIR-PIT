// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Pivot(keycol : String, keyvals : Seq[String], cols : Seq[String]) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField(keycol, StringType) :: cols.map(x=>StructField(x, StringType))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(for {x <- keyvals; y <- cols} yield StructField(x + "_" + y, StringType))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}

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
val step = 10
val mdctn_wide_tables = new ListBuffer[DataFrame]()
println(cols.length + " columns")

for (i <- 0 until cols.length by step) {
  val start = System.nanoTime
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
  mdctn_wide_table.write.format("csv").option("header", true).mode("overwrite").save("/tmp/mdctn_wide/table" + i + ".csv")
  val duration = (System.nanoTime - start) / 1e9d
  println("time " + duration)
}

mdctn_wide.createGlobalTempView("mdctn_wide")

// icd
val icd = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

val icd_wide = icd.groupBy("patient_num", "encounter_num").pivot("concept_cd_instance_num").agg(first("start_date"), first("end_date"))

icd_wide.createGlobalTempView("icd_wide")

// loinc
val loinc = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, modifier_cd, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

val loinc_wide = loinc.groupBy("patient_num", "encounter_num").pivot("concept_cd_instance_num").agg(first("valtype_cd"), first("nval_num"),
  first("tval_char"), first("units_cd"), first("start_date"), first("end_date"))

loinc_wide.createGlobalTempView("loinc_wide")






// vital
val vital = spark.sql("select * from global_temp.observation_fact where concept_cd like 'VITAL:%'")

val vital_wide = vital.groupBy("patient_num", "encounter_num", "instance_num").pivot("concept_cd").agg(collect_list("modifier_cd"), collect_list("valueflag_cd"), collect_list("valtype_cd"), collect_list("nval_num"),
  collect_list("tval_char"), collect_list("units_cd"), collect_list("start_date"), collect_list("end_date"))

vital_wide.createGlobalTempView("vital_wide")
