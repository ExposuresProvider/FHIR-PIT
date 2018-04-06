package datatrans

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import geotrellis.proj4._
import org.apache.commons.text.StringEscapeUtils
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    val fname = form match {
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
        val fname = fn + ".csv"
        val schema = wide.schema
        val (fields2, trans) =
          schema.map(f =>(StructField(f.name, StringType), typeToTrans(f.dataType))).unzip
        val schema2 = StructType(fields2)
        val encoder = RowEncoder(schema2)

        wide.map(row => Row.fromSeq(row.toSeq.zip(trans).map({ case (x, t) => t(x)})))(encoder).write.option("sep", "!").option("header", false).csv(dirn)
        fname

      case JSON =>

        wide.write.json(dirn)
        fn

    }
    val fpath = new Path(fname)
    dfs.delete(fpath, true)
    copyMerge(hc, dfs, true, fname, dpath)
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
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.create (output_file_path)
    writeStringToOutputStream(output_file_output_stream, text)
    output_file_output_stream.close ()
  }

  private def writeStringToOutputStream(output_file_output_stream: FSDataOutputStream, text: String): Unit = {
    val bytes = text.getBytes("utf-8")
    output_file_output_stream.write(bytes)
  }

  def prependStringToFile(hc:Configuration, text : String, path : String) = {
    val input_file_path = new Path(path)
    val input_file_fs = input_file_path.getFileSystem(hc)
    val path2 = path + ".tmp"
    val temp_input_file_path = new Path(path2)
    input_file_fs.rename(input_file_path, temp_input_file_path)

    val output_file_output_stream = input_file_fs.create (input_file_path)

    writeStringToOutputStream(output_file_output_stream, text)
    appendFileToOutputStream(hc, output_file_output_stream, path2)
    output_file_output_stream.close()
    input_file_fs.delete(temp_input_file_path, false)

  }

  def appendStringToFile(hc:Configuration, path :String, text:String) = {
    val bytes = text.getBytes ("utf-8")
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.append (output_file_path)
    output_file_output_stream.write (bytes)
    output_file_output_stream.close ()
  }

  def appendFileToOutputStream(hc : Configuration, output_file_output_stream: FSDataOutputStream, input_file_path: Path) : Unit = {
    val input_file_file_system = input_file_path.getFileSystem(hc)
    val input_file_input_stream = input_file_file_system.open(input_file_path)

    val buf = new Array[Byte](BUFFER_SIZE)

    var n = input_file_input_stream.read(buf)
    while (n != -1) {
      output_file_output_stream.write(buf, 0, n)
      n = input_file_input_stream.read(buf)
    }

    input_file_input_stream.close()
  }

  def appendFileToOutputStream(hc : Configuration, output_file_output_stream: FSDataOutputStream, path2: String) : Unit = {
    val input_file_path = new Path(path2)
    appendFileToOutputStream(hc, output_file_output_stream, input_file_path)
  }

  def appendToFile(hc:Configuration, path :String, path2:String) = {
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.append(output_file_path)
    appendFileToOutputStream(hc, output_file_output_stream, path2)
    output_file_output_stream.close ()
  }

  def to_seq(header: Path, itr: RemoteIterator[LocatedFileStatus]) : Array[Path] = {
    val listBuf = new ListBuffer[Path]
    listBuf.append(header)
    to_seq(listBuf,itr)
    listBuf.toArray
  }

  def to_seq(itr: RemoteIterator[LocatedFileStatus]) : Array[Path] = {
    val listBuf = new ListBuffer[Path]
    to_seq(listBuf, itr)
    listBuf.toArray
  }

  def to_seq(listBuf: ListBuffer[Path], itr: RemoteIterator[LocatedFileStatus]) : Unit = {
    while(itr.hasNext) {
      val fstatus = itr.next()
      listBuf.append(fstatus.getPath)
    }
  }

  def copyMerge(hc: Configuration, output_dir_fs: FileSystem, overwrite: Boolean, output_filename: String, header_file_path: Path, coldir: Path): Boolean = {
    val srcs = to_seq(header_file_path, output_dir_fs.listFiles(coldir, false))
    copyMerge(hc, output_dir_fs, overwrite, output_filename, coldir, srcs)
  }

  def copyMerge(hc: Configuration, output_dir_fs: FileSystem, overwrite: Boolean, output_filename: String, coldir: Path): Boolean = {
    val srcs = to_seq(output_dir_fs.listFiles(coldir, false))
    copyMerge(hc, output_dir_fs, overwrite, output_filename, coldir, srcs)
  }

  private def copyMerge(hc: Configuration, output_dir_fs: FileSystem, overwrite: Boolean, output_filename: String, coldir: Path, srcs: Array[Path]) = {
    val output_file_path = new Path(output_filename)
    val output_file_output_stream = output_dir_fs.create(output_file_path)
    for (p <- srcs) {
      appendFileToOutputStream(hc, output_file_output_stream, p)
    }
    output_file_output_stream.close()
    output_dir_fs.delete(coldir, true)
  }

  def writeHeaderToFile(hc: Configuration, header_filename: String, header: String) = {
    val header_file_path = new Path(header_filename)
    writeToFile(hc, header, header_filename)
    header_file_path
  }

  sealed trait Format
  case object JSON extends Format
  case object CSV extends Format

  def latlon2rowcol(latitude : Double, longitude : Double, year : Int) : Option[(Int, Int)] = {
    // CMAQ uses Lambert Conformal Conic projection
    val proj = "lcc"

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
        val col_no = ((xorig - x1).abs / xcell).floor.toInt + 1
        val row_no = ((yorig.abs + y1) / ycell).floor.toInt + 1
        Some(row_no, col_no)
      } else
        None

    } else
      None

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
  val DATE_FORMAT = "yyyy-MM-dd"
  val BUFFER_SIZE = 4 * 1024

}
