package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

object Utils {

  def time[R](block: =>R) : R = {
    val start = System.nanoTime
    val res = block
    val finish = System.nanoTime
    val duration = (finish - start) / 1e9d
    println("time " + duration + "s")
    res
  }

  def updateMap(m: Map[String, Any], keys : Seq[String], r : Map[String, Any]) : Map[String, Any] = keys match {
    case Seq() => 
      r
    case Seq(k, ks@_*) => 
      val m2 = if (!m.isDefinedAt(k)) Map[String,Any]() else m(k).asInstanceOf[Map[String,Any]]
      m + (k -> updateMap(m2, ks, r))
  }

  def mergeMap(m: Map[String, Any], m2 : Map[String, Any]) : Map[String, Any] = 
    m2.foldLeft(m) {
      case (m3, kv) => 
        kv match {
          case (k,v) =>
            if(v.isInstanceOf[Map[_,_]] && m3.isDefinedAt(k))
              m3 + (k -> mergeMap(m3(k).asInstanceOf[Map[String,Any]], v.asInstanceOf[Map[String,Any]]))
            else
              m3 + (k -> v)
        }
      }

  def flattenMap(m :Map[String, Any]) : Map[Seq[String], Any] = {
    def _flattenMap(keys : Seq[String], m: Map[String,Any]) : Seq[(Seq[String], Any)] =
      m.toSeq.flatMap({case (k, v) => 
        val keys2 = keys :+ k
        if (v.isInstanceOf[Map[_,_]])
          _flattenMap(keys2, v.asInstanceOf[Map[String,Any]])
        else
          Seq((keys2, v))})
    Map(_flattenMap(Seq(), m):_*)
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
  def longToWide(df: DataFrame, indexcols: Seq[String], keycols : Seq[String], cols : Seq[String], col:String) : DataFrame = {
    println("processing " + col)
    val nkeyvals = df.select(keycols.map(x=>df(x)):_*).distinct.count //rdd.map(r => r.togetString(0)).collect.toSeq
    val nrows = df.count

    println(nrows + " rows")
    println(nkeyvals + " columns")

    val pivot = new Pivot(
      keycols,
      cols
    )

    df.groupBy(indexcols.map(x => df.col(x)) : _*).agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }


  def aggregate(df: DataFrame, keycols : Seq[String], cols : Seq[String], col:String) : DataFrame = {
    println("processing " + col)
    val nkeyvals = df.select(keycols.map(x=>df(x)):_*).distinct.count //rdd.map(r => r.togetString(0)).collect.toSeq
    val nrows = df.count

    println(nrows + " rows")
    println(nkeyvals + " columns")

    val pivot = new Pivot(
      keycols,
      cols
    )

    df.agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }

  sealed trait Format
  case object JSON extends Format
  case object CSV extends Format


}
