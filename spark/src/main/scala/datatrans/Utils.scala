package datatrans

import java.io._
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.circe._
import io.circe.yaml.parser.{parse}
import geotrellis.proj4.{CRS, Transform, LatLng}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.apache.commons.csv._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.ref.SoftReference

import scala.collection.mutable.Map
import java.security.MessageDigest
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

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
    if (x == null)
      ""
    else
      ty match {
        case IntegerType => x.toString
        case DoubleType => x.toString
        case StringType => "\"" + StringEscapeUtils.escapeCsv(x.asInstanceOf[String]) + "\""
        case MapType(_, _, _) => flattenMap(x.asInstanceOf[Map[String, Any]]).map({ case (k, v) => typeToTrans(StringType)(k.mkString("_")) + ":" + typeToTrans(StringType)(v) }).mkString("{", ",", "}")
        case ArrayType(et, _) => x.asInstanceOf[Seq[_]].map(typeToTrans(et)).mkString("[", ",", "]")
        case StructType(fs) => (x.asInstanceOf[Row].toSeq zip fs).map({ case (x2, f) => typeToTrans(f.dataType)(x2) }).mkString("(", ",", ")")
        case _ => throw new RuntimeException("typeToTrans: unsupported data type " + ty)
      }

  def writeCSV(spark:SparkSession, wide:DataFrame, fn:String, form:Format, skipCRC : Boolean = true) : Unit = {

    val hc = spark.sparkContext.hadoopConfiguration
    val dirn = fn + "_temp"
    val dpath = new Path(dirn)
    val dfs = dpath.getFileSystem(hc)

    val fname = form match {
      case CSV(sep) =>
        val fname = fn + ".csv"
        val schema = wide.schema
        val (fields2, trans) =
          schema.map(f =>(StructField(f.name, StringType), typeToTrans(f.dataType))).unzip
        val schema2 = StructType(fields2)
        val encoder = RowEncoder(schema2)

        wide.map(row => Row.fromSeq(row.toSeq.zip(trans).map({ case (x, t) => t(x)})))(encoder).write.option("sep", sep).option("header", value = false).csv(dirn)
        fname

      case JSON =>

        wide.write.json(dirn)
        fn

    }
    val fpath = new Path(fname)
    dfs.delete(fpath, true)
    copyMerge(hc, dfs, overwrite = true, fname, dpath)

    form match {
      case CSV(sep) =>
        prependStringToFile(hc, wide.columns.mkString(sep) + "\n", fname)
      case JSON =>
    }
    if(skipCRC)
      deleteCRCFile(dfs, fpath)
  }

  def readCSV(spark: SparkSession, fn: String, schema : StructType, field_type : String => DataType = s => throw new RuntimeException(s"readCSV: Cannot find $s in schema")): DataFrame = {
    val fileSchema = spark.read.option("header", "true").csv(fn).schema
    val fieldNames = fileSchema.fieldNames.toSeq
    val fieldNameSet = fieldNames.toSet
    val fields = schema.fields.toSeq
    val fieldsNotInFile = fields.filter((x : StructField) => !fieldNameSet.contains(x.name))
    val fieldsInFile = fieldNames.map((s : String) => fields.find((x: StructField) => x.name == s) match {
      case Some(y) => y
      case None => StructField(s, field_type(s))
    })
    val schemaFiltered = StructType(fieldsInFile ++ fieldsNotInFile)
    spark.read.format("csv").option("header", value = true).schema(schemaFiltered).load(fn)
  }

  def readCSV2(spark: SparkSession, fn: String, schema : StructType, field_type : String => DataType = s => throw new RuntimeException(s"readCSV: Cannot find $s in schema")): DataFrame = {
    val csv0 = spark.read.option("header", "true").csv(fn)
    val fileSchema = csv0.schema
    val fieldNames = fileSchema.fieldNames.toSeq
    val fieldNameSet = fieldNames.toSet
    val fields = schema.fields.toSeq
    val fieldsNotInFile = fields.filter((x : StructField) => !fieldNameSet.contains(x.name))
    val fieldsInFile = fieldNames.map((s : String) => fields.find((x: StructField) => x.name == s) match {
      case Some(y) => y
      case None => StructField(s, field_type(s))
    })
    val schemaFiltered = StructType(fieldsInFile)
    fieldsNotInFile.foldLeft(csv0)((csv1:DataFrame, f : StructField)=> csv1.withColumn(f.name, lit(null).cast(f.dataType)))
  }


  def writeDataframe(hc: Configuration, output_file: String, table: DataFrame, skipCRC : Boolean = true): Unit = {
    val dname = output_file + "_temp"
    val dpath = new Path(dname)
    table.write.option("sep", ",").option("header", value = false).csv(dname)

    val output_file_path = new Path(output_file)
    val output_file_file_system = output_file_path.getFileSystem(hc)
    output_file_file_system.delete(output_file_path, true)
    copyMerge(hc, output_file_file_system, overwrite = true, output_file, dpath)

    prependStringToFile(hc, table.columns.mkString(",") + "\n", output_file)
    if(skipCRC)
      deleteCRCFile(output_file_file_system, output_file_path)
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

  def writeToFile(hc:Configuration, path :String, text :String) : Unit = {
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

  def prependStringToFile(hc:Configuration, text : String, path : String) : Unit = {
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

  def appendStringToFile(hc:Configuration, path :String, text:String) : Unit = {
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

  def appendToFile(hc:Configuration, path :String, path2:String) : Unit = {
    val output_file_path = new Path (path)
    val output_file_file_system = output_file_path.getFileSystem (hc)
    val output_file_output_stream = output_file_file_system.append(output_file_path)
    appendFileToOutputStream(hc, output_file_output_stream, path2)
    output_file_output_stream.close ()
  }

  def to_seq(header: Path, itr: RemoteIterator[LocatedFileStatus]) : Seq[Path] = {
    val listBuf = new ListBuffer[Path]
    listBuf.append(header)
    to_seq(listBuf,itr)
    listBuf
  }

  def to_seq(itr: RemoteIterator[LocatedFileStatus]) : Seq[Path] = {
    val listBuf = new ListBuffer[Path]
    to_seq(listBuf, itr)
    listBuf
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

  private def copyMerge(hc: Configuration, output_dir_fs: FileSystem, overwrite: Boolean, output_filename: String, coldir: Path, srcs: Seq[Path]) = {
    val output_file_path = new Path(output_filename)
    val output_file_output_stream = output_dir_fs.create(output_file_path)
    for (p <- srcs) {
      appendFileToOutputStream(hc, output_file_output_stream, p)
    }
    output_file_output_stream.close()
    output_dir_fs.delete(coldir, true)
  }

  sealed trait Format
  case object JSON extends Format
  case class CSV(sep: String) extends Format

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
  // def insertOrUpdate(mmap: mutable.Map[DateTime, JsObject], key: DateTime, col: String, value: JsValue): Unit = {
  //   mmap.get(key) match {
  //     case Some(_) =>
  //       val json = mmap(key)
  //       json \ col match {
  //         case JsDefined(value0) =>
  //           value0 match {
  //             case JsArray(arr) =>
  //               mmap(key) ++= Json.obj(col -> (arr ++ value.asInstanceOf[JsArray].value))
  //             case JsNumber(n) =>
  //               mmap(key) ++= Json.obj(col -> (n + value.asInstanceOf[JsNumber].value))
  //             case _ =>
  //               if (value0 != value) {
  //                 throw new RuntimeException("the key " + col + " is mapped to different values " + value0 + " " + value)
  //               }
  //           }
  //         case _ =>
  //           mmap(key) ++= Json.obj(col -> value)
  //       }
  //     case None =>
  //       mmap(key) = Json.obj(col -> value)
  //   }
  // }
  val DATE_FORMAT = "yyyy-MM-dd"
  val BUFFER_SIZE : Int = 4 * 1024

  // def extractField(jsvalue: JsValue, field: String) : Option[JsValue] = {
  //   jsvalue \ field match {
  //     case JsUndefined() =>
  //       None
  //     case JsDefined(lat) =>
  //       Some(lat)
  //   }
  // }

  def patientDimension(spark: SparkSession, hc: Configuration, patient_dimension: Option[String], time_series: String) = {
    patient_dimension match {
      case Some(pd) =>
        println("loading patient_dimension from " + pd)

        spark.read.format("csv").option("header", value = true).load(pd)
      case None =>
        import spark.implicits._
        val input_file = time_series
        val input_file_path = new Path(input_file)
        val input_file_file_system = input_file_path.getFileSystem(hc)
        val files = input_file_file_system.listStatus(input_file_path, new PathFilter {
          override def accept(path: Path) : Boolean = input_file_file_system.isFile(path)
        }).map(fs => fs.getPath.getName)
        files.toSeq.toDF("patient_num")
    }
  }

  case class HDFSCollection(hc: Configuration, path : Path) extends Traversable[Path] {
    override def foreach[U](f : Path => U) = {
      val fs = path.getFileSystem(hc)
      val file_iter = fs.listFiles(path, false)
      while(file_iter.hasNext) {
        val file_status = file_iter.next()
        f(file_status.getPath)
      }
    }
  }

  def deleteCRCFile(fs: FileSystem, path: Path) : Unit = {
    val dir = path.getParent()
    val name = path.getName()
    val crc = new Path(dir, "." + name + ".crc")
    if (fs.exists(crc))
      fs.delete(crc, false)
  }

  def loadJson[U](hc: Configuration, path: Path)(implicit codec : JsonValueCodec[U]) : U = {
    val fs = path.getFileSystem(hc)
    val is = fs.open(path)
    val obj = try { readFromStream[U](is) } finally { is.close() }
    obj
  }

  def saveJson[U](hc: Configuration, path: Path, json: U, skipCRC : Boolean = true)(implicit codec : JsonValueCodec[U]) : Unit = {
    val fs = path.getFileSystem(hc)
    val is = fs.create(path)
    val obj = try { writeToStream[U](json, is) } finally {
      is.close()
      if(skipCRC)
        deleteCRCFile(fs, path)
    }
  }

class Cache[K,V <: AnyRef](fun : K => V) {
  private val cache = Map[K, SoftReference[V]]()
  def apply(key: K) : V = {
    this.synchronized {
      def store = {
        val df = fun(key)
        cache(key) = new SoftReference(df)
        println("SoftReference created for " + key)
        df
      }

      cache.get(key) match {
        case None =>
          store
        case Some(x) =>
          x.get match {
            case Some(df) => df
            case None =>
              println("SoftReference has already be garbage collected " + key)
              store

          }
      }
    }
  }
}
  def withCounter[U](a: AtomicInteger => U) : U = {
    val count = new AtomicInteger(0)
    a(count)
  }


  def combineCSVs(hc: Configuration, input_directory: String, output_file: String, skipCRC : Boolean = true) : Unit = {
          val output_directory_path = new Path(input_directory)
          val output_directory_file_system = output_directory_path.getFileSystem(hc)

          val files = new HDFSCollection(hc, output_directory_path).toSeq
          if (!files.isEmpty) {
            println("find columns")

            val colnames = withCounter(count =>
              files.map(f => {
                println("loading " + count.incrementAndGet + " " + f)
                val csvParser = new CSVParser(new InputStreamReader(output_directory_file_system.open(f), "UTF-8"), CSVFormat.DEFAULT
                  .withFirstRecordAsHeader()
                  .withIgnoreHeaderCase()
                  .withTrim())
                try {
                  csvParser.getHeaderMap().keySet().asScala
                } finally {
                  csvParser.close()
                }
              })
            ).reduce((df1, df2) => df1 ++ df2).toSeq

            println("extend dataframes")
            val output_file_path = new Path(output_file)
            val output_file_csv_writer = new CSVPrinter( new OutputStreamWriter(output_directory_file_system.create(output_file_path), "UTF-8" ) , CSVFormat.DEFAULT.withHeader(colnames:_*))

            try {
              withCounter(count =>
                files.foreach(f => {
                  println("loading " + count.incrementAndGet + " " + f)
                  val csvParser = new CSVParser(new InputStreamReader(output_directory_file_system.open(f), "UTF-8"), CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim())
                  try {
                    val hdrmap = csvParser.getHeaderMap().asScala
                    val buf = Array(colnames.size)
                    csvParser.asScala.foreach(rec => {
                      val rec2 = colnames.map(co =>
                        hdrmap.get(co) match {
                          case Some(x) => rec.get(x)
                          case None => ""
                        }
                      ).asJava
                      output_file_csv_writer.printRecord(rec2)
                    })
                  } finally {
                    csvParser.close()
                  }
                })
              )
            } finally {
              output_file_csv_writer.close()
              if(skipCRC)
                deleteCRCFile(output_directory_file_system, output_file_path)
            }
          }
  }

  def fileExists(hc: Configuration, file : String) : Boolean = {
    val output_file_path = new Path(file)
    val output_file_file_system = output_file_path.getFileSystem(hc)
    output_file_file_system.exists(output_file_path)

  }

  def readFile(hc: Configuration, file : String) : String = {
    val path = new Path(file)
    val file_system = path.getFileSystem(hc)
    val input_stream = file_system.open(path)
    val source = scala.io.Source.fromInputStream(input_stream, "utf-8")
    try source.mkString finally {
      source.close()
      input_stream.close()
    }
  }

  def addColumnIndex(spark: SparkSession, df: DataFrame) = spark.createDataFrame(
    // Add Column index
    df.rdd.zipWithIndex.map{case (row, columnindex) => Row.fromSeq(row.toSeq :+ columnindex)},
    // Create schema
    StructType(df.schema.fields :+ StructField("columnindex", LongType, false))
  )

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }


  def parseInputStream(input_file_input_stream : InputStream) : Json = {
    val str = scala.io.Source.fromInputStream(input_file_input_stream, "utf-8").mkString

    io.circe.parser.parse(str) match {
      case Left(error) => throw new RuntimeException(error)
      case Right(obj) => obj
    }
  }

  def parseInputStreamYaml(input_file_input_stream : InputStream) : Json = {
    val str = scala.io.Source.fromInputStream(input_file_input_stream, "utf-8").mkString

    parse(str) match {
      case Left(error) => throw new RuntimeException(error)
      case Right(obj) => obj
    }
  }

  def stringToDateTime(str : String, dateTimeParser : DateTimeFormatter = ISODateTimeFormat.dateTimeParser()) : DateTime = DateTime.parse(str, dateTimeParser)

  def feature_type_to_sql_type(feature_type: String): DataType = feature_type match {
    case "integer" => IntegerType
    case "number" => DoubleType
    case "string" => StringType
  }

}
