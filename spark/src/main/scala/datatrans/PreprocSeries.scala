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

object PreprocSeries {

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

    df.groupBy("patient_num").agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }

  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

//      spark.sparkContext.setLogLevel("WARN")

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
      println("loading observation_fact from " + pdif)
      val ofdf = spark.read.format("csv").option("header", true).load(ofif)

      val inout = vddf.select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

      val pat = pddf.select("patient_num", "race_cd", "sex_cd", "birth_date")

      val lat = ofdf.filter($"concept_cd".like("GEO:LAT")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lat"))

      val lon = ofdf.filter($"concept_cd".like("GEO:LONG")).select("patient_num", "nval_num").groupBy("patient_num").agg(avg("nval_num").as("lon"))

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
        val observation = ofdf.select("patient_num", "encounter_num", "concept_cd", "instance_num", "modifier_cd", "valueflag_cd", "valtype_cd", "nval_num", "tval_char", "units_cd", "start_date", "end_date")

        val observation_wide = longToWide(observation, Seq("encounter_num", "concept_cd", "instance_num", "modifier_cd"), cols, "observation")

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
        val visit = vddf.select("patient_num", "encounter_num", "inout_cd", "start_date", "end_date")

        val visit_wide = longToWide(visit, Seq("encounter_num"), cols, "visit")

        visit_wide.persist(StorageLevel.MEMORY_AND_DISK)

        visit_wide
      }

//      val merge_map = udf((a:Map[String,Any], b:Map[String,Any]) => mergeMap(a,b))

      val features_wide = features
        .join(observation_wide, Seq("patient_num"))
        .join(visit_wide, Seq("patient_num"))
//        .select($"patient_num", $"encounter_num", $"sex_cd", $"race_cd", $"birth_date", merge_map($"observation", $"visit"))

      writeCSV(spark, features_wide, output_path,form)

      spark.stop()
    }
  }
}
