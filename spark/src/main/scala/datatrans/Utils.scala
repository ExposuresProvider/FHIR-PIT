package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import geotrellis.proj4._
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection.mutable

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

  def aggregate(df: DataFrame, keycols : Seq[String], cols : Seq[String], col:String) : DataFrame = {
    println("processing " + col)

    val pivot = new Pivot(
      keycols,
      cols
    )

    df.agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }

  def groupByAndAggregate(df: DataFrame, groupcols: Seq[String], keycols : Seq[String], cols : Seq[String], col:String) : DataFrame = {
    println("processing " + col)

    val pivot = new Pivot(
      keycols,
      cols
    )

    df.groupBy(groupcols.map(x => df.col(x)) : _*).agg(pivot(
      (keycols ++ cols).map(x => df.col(x)) : _*
    ).as(col))

  }

  def writeToFile(hc:Configuration, path :String, text :String) = {
    val bytes = text.getBytes ("utf-8")
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.create (output_file_path)
    output_file_output_stream.write (bytes)
    output_file_output_stream.close ()
  }

  def appendStringToFile(hc:Configuration, path :String, text:String) = {
    val bytes = text.getBytes ("utf-8")
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.append (output_file_path)
    output_file_output_stream.write (bytes)
    output_file_output_stream.close ()
  }

  def appendToFile(hc:Configuration, path :String, path2:String) = {
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)

    val input_file_path = new Path (path2)
    val input_file_file_system = input_file_path.getFileSystem (hc)

    val output_file_output_stream = output_file_file_system.append(output_file_path)
    val input_file_input_stream = input_file_file_system.open(input_file_path)

    val buf = new Array[Byte](4 * 1024)

    var n = input_file_input_stream.read(buf)
    while(n != -1) {
      output_file_output_stream.write (buf, 0, n)
      n = input_file_input_stream.read(buf)
    }

    input_file_input_stream.close()
    output_file_output_stream.close ()
  }

  sealed trait Format
  case object JSON extends Format
  case object CSV extends Format

  def latlon2rowcol(latitude : Double, longitude : Double, year : Int) : (Int, Int) = {
    // CMAQ uses Lambert Conformal Conic projection
    val proj = "lcc"
    var row_no = -1
    var col_no = -1

    if (year == 2010 || year == 2011) {
      // open CMAQ 12K NetCDF file (47Gb!!)
      // dataset = Dataset('./projects/datatrans/CMAQ/2011/CCTM_CMAQ_v51_Release_Oct23_NoDust_ed_emis_combine.aconc.01')
      // print(dataset.file_format)

      // pull out some attributes we need
      // sdate = getattr(dataset, 'SDATE')
      // stime = getattr(dataset, 'STIME')
      // number_of_columns = getattr(dataset, 'NCOLS') # 459
      // number_of_rows = getattr(dataset, 'NROWS') # 299
      // lat_0 = getattr(dataset, 'YCENT') # 40.0
      // lat_1 = getattr(dataset, 'P_ALP') # 33.0
      // lat_2 = getattr(dataset, 'P_BET') # 45.0
      // lon_0 = getattr(dataset, 'XCENT') # -97
      // xorig = getattr(dataset, 'XORIG') # -2556000.0
      // yorig = getattr(dataset, 'YORIG') # -1728000.0
      // xcell = getattr(dataset, 'XCELL') # 12000.0
      // ycell = getattr(dataset, 'YCELL') # 12000.0
      // dataset.close()


      // NEED TO CHANGE THIS TO GET PROJECTION VALUES FROM CONFIG FILE or DB?
      var number_of_columns = 0
      var number_of_rows = 0
      var xcell : Double = 0
      var ycell : Double = 0
      if(year == 2010) {
        number_of_columns = 148
        number_of_rows = 112
        xcell = 36000.0
        ycell = 36000.0
      } else {
        number_of_columns = 459
        number_of_rows = 299
        xcell = 12000.0
        ycell = 12000.0
      }

      val lat_0 = "40.0"
      val lat_1 = "33.0"
      val lat_2 = "45.0"
      val lon_0 = "-97"
      val xorig = -2556000.0
      val yorig = -1728000.0

      // Create the CMAQ projection so we can do some conversions
      val proj_str = "+proj=" + proj + " +lon_0=" + lon_0 + " +lat_0=" + lat_0 + " +lat_1=" + lat_1 + " +lat_2=" + lat_2 + " +units=meters"

      val lcc = CRS.fromString(proj_str)

      val p1 = Transform(LatLng, lcc)

      val (x1, y1) = p1(longitude, latitude)

      // verify the points are in the grid
      if ((x1 >= xorig) && (x1 <= (xorig + (xcell * number_of_columns))) &&
        (y1 >= yorig) && (y1 <= (yorig + (ycell * number_of_rows)))) {
        // find row and column in grid
        col_no = ((xorig - x1).abs / xcell).floor.toInt + 1
        row_no = ((yorig.abs + y1) / ycell).floor.toInt + 1
      }

    }


    return Tuple2(row_no, col_no)

  }

  def insertOrUpdate(mmap: mutable.Map[DateTime, JsObject], key: DateTime, col: String, value: JsValue) = {
    mmap.get(key) match {
      case Some(vec) =>
        val json = mmap(key)
        json \ col match {
          case JsDefined(value0) =>
            value0 match {
              case JsArray(arr) =>
                mmap(key) ++= Json.obj(col -> (arr ++ value.asInstanceOf[JsArray].value))
              case JsNumber(n) =>
                mmap(key) ++= Json.obj(col -> (n + value.asInstanceOf[JsNumber].value))
              case _ =>
                if (value0 != value) {
                  throw new RuntimeException("the key " + col + " is mapped to different values " + value0 + " " + value)
                }
            }
          case _ =>
            mmap(key) ++= Json.obj(col -> value)
        }
      case None =>
        mmap(key) = Json.obj(col -> value)
    }
  }

}
