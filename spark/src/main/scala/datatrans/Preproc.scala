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

  sealed trait Format
  case object JSON extends Format
  case object CSV extends Format

  def time[R](block: =>R) : R = {
    val start = System.nanoTime
    val res = block
    val finish = System.nanoTime
    val duration = (finish - start) / 1e9d
    println("time " + duration + "s")
    res
  }

  def typeToTrans(ty : DataType) : Any=>String = (x:Any)=>
    if (x == null) {
      ""
    } else {

      ty match {
        case IntegerType => x.toString
        case DoubleType => x.toString
        case StringType => "\"" + StringEscapeUtils.escapeCsv(x.asInstanceOf[String]) + "\""
        case MapType(_,_,_) => flattenMap(x.asInstanceOf[Map[String,Any]]).map({case (k,v) => typeToTrans(StringType)(k.mkString("_")) + ":" + typeToTrans(StringType)(v)}).mkString("{",",","}")
        case ArrayType(et,_) => x.asInstanceOf[Seq[_]].map(typeToTrans(et)).mkString("[",",","]")
        case StructType(fs) => (x.asInstanceOf[Row].toSeq zip fs).map({ case (x, f)=>typeToTrans(f.dataType)(x)}).mkString("(",",",")")
        case _ => throw new RuntimeException("typeToTrans: unsupported data type " + ty)
      }
    }
  

  def writeCSV(spark:SparkSession, wide:DataFrame, fn:String, form:Format) : Unit = {

    val hc = spark.sparkContext.hadoopConfiguration
    val dirn = fn + "_temp"
    val dpath = new Path(dirn)
    val dfs = dpath.getFileSystem(hc)

    form match {
      case CSV =>
        val mn = fn + "_meta.csv"
        val mpath = new Path(mn)
        val mfs = mpath.getFileSystem(hc)
        if(mfs.exists(mpath)) {
          mfs.delete(mpath, true)
        }

        val mout = new BufferedWriter(new OutputStreamWriter(mfs.create(mpath)))
        mout.write(wide.columns.mkString("!"))
        mout.close
        val fpath = new Path(fn + ".csv")
        val ffs = fpath.getFileSystem(hc)
        val schema = wide.schema
        val (fields2, trans) = 
          schema.map(f =>(StructField(f.name, StringType), typeToTrans(f.dataType))).unzip
        val schema2 = StructType(fields2)
        val encoder = RowEncoder(schema2)

        wide.map(row => Row.fromSeq(row.toSeq.zip(trans).map({ case (x, t) => t(x)})))(encoder).write.option("sep", "!").option("header", false).csv(dirn)
        ffs.delete(fpath, true)
        FileUtil.copyMerge(dfs, dpath, ffs, fpath, true, hc, null)

      case JSON =>
        val fpath = new Path(fn)
        val ffs = fpath.getFileSystem(hc)
        wide.write.json(dirn)
        ffs.delete(fpath, true)
        FileUtil.copyMerge(dfs, dpath, ffs, fpath, true, hc, null)

    }
  }

  def longToWide(df: DataFrame, keycols : Seq[String], cols : Seq[String], col:String) : DataFrame = {
    println("processing " + col)
    val nkeyvals = df.select(keycols.map(x=>df(x)):_*).distinct.count //rdd.map(r => r.togetString(0)).collect.toSeq
    val nrows = df.count

    println(nrows + " rows")
    println(nkeyvals + " columns")

    val pivot = new Pivot(
      keycols,
      cols
    )

    df.groupBy("patient_num", "encounter_num").agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }

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

        val mdctn_wide = longToWide(mdctn, Seq("concept_cd", "instance_num", "modifier_cd"), cols, "mdctn")

        mdctn_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, mdctn_wide, "/tmp/mdctn_wide",form)

        mdctn_wide
      }

      // icd
      val icd_wide = time {
        val icd = spark.sql("select patient_num, encounter_num, concept_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'ICD%'")

        val icd_wide = longToWide(icd, Seq("concept_cd"), Seq("start_date", "end_date"), "icd")

        icd_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, icd_wide, "/tmp/icd_wide",form)

        icd_wide
      }

      // loinc
      val loinc_wide = time {
        val loinc = spark.sql("select patient_num, encounter_num, concept_cd, instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'LOINC:%'")

        val loinc_wide = longToWide(loinc, Seq("concept_cd", "instance_num"), cols, "loinc")

        loinc_wide.persist(StorageLevel.MEMORY_AND_DISK)

//        writeCSV(spark, loinc_wide, "/tmp/loinc_wide",form)

        loinc_wide
      }

      // vital
      val vital_wide = time {
        val vital = spark.sql("select patient_num, encounter_num, concept_cd, instance_num, valueflag_cd, valtype_cd, nval_num, tval_char, units_cd, start_date, end_date from global_temp.observation_fact where concept_cd like 'VITAL:%'")

        val vital_wide = longToWide(vital, Seq("concept_cd", "instance_num"), cols, "vital")

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
