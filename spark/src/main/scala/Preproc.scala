// http://ensime.github.io/editors/emacs/scala-mode/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.hadoop.conf.Configuration


object Preproc {

  def time[R](block: =>R) : R = {
    val start = System.nanoTime
    val res = block
    val finish = System.nanoTime
    val duration = (finish - start) / 1e9d
    println("time " + duration + "s")
    res
  }

  def writeCSV(spark:SparkSession, wide:DataFrame, fn:String) : Unit = {

    val hc = spark.sparkContext.hadoopConfiguration
    val fpath = new Path(fn)
    val ffs = fpath.getFileSystem(hc)
    val mn = fn + "_meta"
    val mpath = new Path(mn)
    val mfs = mpath.getFileSystem(hc)
    val dirn = fn + "_temp"
    val dpath = new Path(dirn)
    val dfs = dpath.getFileSystem(hc)
    val mdirn = mn + "_temp"
    val mdpath = new Path(mdirn)
    val mdfs = dpath.getFileSystem(hc)

    wide.limit(0).write.option("sep", "!").option("header", true).csv(mdirn)
    mfs.delete(mpath, true)
    FileUtil.copyMerge(mdfs, mdpath, mfs, mpath, true, hc, null)

    wide.write.option("sep", "!").option("header", false).csv(dirn)
    ffs.delete(fpath, true)
    FileUtil.copyMerge(dfs, dpath, ffs, fpath, true, hc, null)

  }

  def meta(keyvals:Seq[String], cols:Seq[String]) : Seq[String] = for {x <- keyvals; y <- cols} yield x + "_" + y

  def longToWide(df: DataFrame, keycol : String, cols : Seq[String], col:String) : DataFrame = {
    val keyvals = df.select(keycol).distinct.rdd.map(r => r.getString(0)).collect.toSeq


    println("processing " + col)
    println(keyvals.length + " columns")
    time {
      val pivot = new Pivot(
        keycol,
        keyvals,
        cols
      )

      df.groupBy("patient_num", "encounter_num").agg(to_json(pivot(
        cols.map(x => df.col(x)) : _*
      )).as(col))

    } 

  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()
    // val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

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

    val mdctn_wide = longToWide(mdctn, "concept_cd_modifier_cd_instance_num", cols, "mdctn")

    mdctn_wide.persist(StorageLevel.MEMORY_AND_DISK)

    writeCSV(spark, mdctn_wide, "/tmp/mdctn_wide.csv")

    // icd
    val icd = spark.sql("select patient_num, encounter_num, concept_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

    val icd_wide = longToWide(icd, "concept_cd", Seq("start_date", "end_date"), "icd")

    icd_wide.persist(StorageLevel.MEMORY_AND_DISK)

    writeCSV(spark, icd_wide, "/tmp/icd_wide.csv")

    // loinc
    val loinc = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

    val loinc_wide = longToWide(loinc, "concept_cd_instance_num", cols, "loinc")

    loinc_wide.persist(StorageLevel.MEMORY_AND_DISK)

    writeCSV(spark, loinc_wide, "/tmp/loinc_wide.csv")

    // vital
    val vital = spark.sql("select patient_num, encounter_num, concat(concept_cd, '_', instance_num) concept_cd_instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'VITAL:%'")

    val vital_wide = longToWide(vital, "concept_cd_instance_num", cols, "vital")

    vital_wide.persist(StorageLevel.MEMORY_AND_DISK)

    writeCSV(spark, vital_wide, "/tmp/vital_wide.csv")

    val inout = vddf.select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

    val pat = pddf.select("patient_num", "race_cd", "sex_cd", "birth_date")

    val lat = ofdf.filter($"concept_cd".like("GEO:LAT")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lat"))

    val lon = ofdf.filter($"concept_cd".like("GEO:LONG")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lon"))

    val features = pat
      .join(inout, "patient_num")
      .join(lat, "patient_num")
      .join(lon, "patient_num")

    features.persist(StorageLevel.MEMORY_AND_DISK);

    writeCSV(spark, features, "/tmp/features.csv")

    val features_wide = features
      .join(icd_wide, Seq("patient_num", "encounter_num"))
      .join(loinc_wide, Seq("patient_num", "encounter_num"))
      .join(mdctn_wide, Seq("patient_num", "encounter_num"))
      .join(vital_wide, Seq("patient_num", "encounter_num"))

    writeCSV(spark, features_wide, "/tmp/features_wide.csv")

    spark.stop()
  }
}
