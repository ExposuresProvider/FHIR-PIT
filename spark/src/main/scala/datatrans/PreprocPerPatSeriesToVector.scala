package datatrans

import java.util.concurrent.atomic.AtomicInteger

import scala.ref.SoftReference
import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json.JsValueWrapper
import scopt._

case class Config(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : String = "",
                   time_series : String = "",
                   environmental_data : Option[String] = None,
                   output_prefix : String = "",
                   start_date : Option[DateTime] = None,
                   end_date : Option[DateTime] = None,
                   regex_observation : Option[String] = None,
                   regex_observation_filter_visit : Option[String] = None,
                   regex_visit : Option[String] = None,
                   map : Option[String] = None,
                   aggregate_by : Option[String] = None,
                   output_format : String = "json",
                   debug : Boolean = false
                 )

object PreprocPerPatSeriesToVector {
  def loadEnvData(config : Config, spark: SparkSession, lat: Double, lon:Double, start_date: DateTime, indices : Seq[String], statistics : Seq[String]) = {

    var env = Json.obj()
    val names = for(i <- statistics; j <- indices) yield f"${j}_$i"

    for(i <- -7 to 7) {
      val start_time = start_date.plusDays(i)

      loadDailyEnvData(config, spark, lat, lon, start_time, names) match {
        case Some(obj) =>
          env ++= Json.obj((if (config.debug) "row" +: ("col" +: ("start_date" +: names)) else names).map(x => x + "_day" + i -> (obj(x) : JsValueWrapper)) : _*)
        case None =>
      }
    }

    env
  }

  val cache = scala.collection.mutable.Map[String, SoftReference[DataFrame]]()

  def loadDailyEnvData(config : Config, spark: SparkSession, lat: Double, lon:Double, start_date: DateTime, names : Seq[String]) : Option[JsObject] = {
    val year = start_date.year.get
    val (row, col) = latlon2rowcol(lat, lon, year)

    if (row == -1 || col == -1) {
      None
    } else {
      val filename = f"${config.input_directory}/${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"
      def loadEnvDataFrame(filename : String) = {
        val df = spark.read.format("csv").load(filename).toDF(("a" +: names) : _*)
        cache(filename) = new SoftReference(df)
        println("SoftReference created for " + filename)
        df
      }
      val df = cache.get(filename) match {
        case None =>
          loadEnvDataFrame(filename)
        case Some(x) =>
          x.get.getOrElse {
            println("SoftReference has already be garbage collected " + filename)
            loadEnvDataFrame(filename)
          }
      }
      val aggregatedf = df.filter(df("a") === start_date.toString("yyyy-MM-dd")).select(names.map(df.col) : _*)
      if (aggregatedf.count == 0) {
        println("env data not found" + " " + "row " + row + " col " + col + " start_date " + start_date.toString("yyyy-MM-dd"))
        None
      } else {
        val aggregate = aggregatedf.first
        val tuples = (0 until names.size).map(i => names(i) -> (aggregate.getString(i).toDouble: JsValueWrapper))
        Some(Json.obj(
          (if(config.debug)
            ("row" -> (row : JsValueWrapper)) +: (("col" -> (col : JsValueWrapper)) +: (("start_date" -> (start_date.toString("yyyy-MM-dd") : JsValueWrapper)) +: tuples))
          else
            tuples) : _*))
      }

    }
  }

  def proc_pid(config : Config, spark: SparkSession, p:String, col_filter_observation: (String, DateTime) => (Boolean, Seq[(String, JsValue)]), col_filter_visit: (String, DateTime) => Seq[(String, JsValue)], crit : JsObject => Boolean) =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = f"${config.input_directory}/${config.time_series}/$p"
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

      val output_file = config.output_prefix + p
      val output_file_path = new Path(output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {

        if(!input_file_file_system.exists(input_file_path)) {
          println("json not found, skipped " + p)
        } else {
          println("loading json from " + input_file)
          val input_file_input_stream = input_file_file_system.open(input_file_path)

          val jsvalue = Json.parse(input_file_input_stream)
          input_file_input_stream.close()
          val listBuf = scala.collection.mutable.Map[DateTime, JsObject]() // a list of concept, start_time

          val visits = jsvalue("visit").as[JsObject]
          val observations = jsvalue("observation").as[JsObject]
          val sex_cd = jsvalue("sex_cd").as[String]
          val race_cd = jsvalue("race_cd").as[String]
          val lat = jsvalue("lat").as[Double]
          val lon = jsvalue("lon").as[Double]

          val start_date_set = scala.collection.mutable.Set[DateTime]()

          jsvalue \ "birth_date" match {
            case JsDefined (bd) =>
              val birth_date = bd.as[String]
              val birth_date_joda = DateTime.parse (birth_date, DateTimeFormat.forPattern("M/d/y H:m"))

              val encounters = observations.fields
              encounters.foreach {
                case (_, encounter) =>
                  encounter.as[JsObject].fields.foreach {
                    case (concept_cd, instances) =>
                      instances.as[JsObject].fields.foreach {
                        case (_, modifiers) =>
                          val start_date = DateTime.parse (modifiers.as[JsObject].values.toSeq (0) ("start_date").as[String], DateTimeFormat.forPattern("Y-M-d H:m:s") )
                          val (start_date_filtered, cols) = col_filter_observation(concept_cd, start_date)
                          if(start_date_filtered) {
                            start_date_set.add(start_date)
                          }
                          cols.foreach {
                            case (col, value) =>
                              insertOrUpdate(listBuf, start_date, col, value)
                          }

                      }
                  }
              }

              val encounters_visit = visits.fields
              encounters_visit.foreach {
                case (visit, encounter) =>
                  encounter \ "start_date" match {
                    case JsDefined (x) =>
                      val start_date = DateTime.parse (x.as[String], DateTimeFormat.forPattern("y-M-d H:m:s") )
                      if(config.regex_observation_filter_visit.isEmpty || start_date_set.contains(start_date)) {
                        encounter \ "inout_cd" match {
                          case JsDefined (y) =>
                            val inout_cd = y.as[String]
                            col_filter_visit(inout_cd, start_date).foreach {
                              case (col, value) =>
                                insertOrUpdate(listBuf, start_date, col, value)
                            }
                          case _ =>
                            println ("no inout cd " + visit)
                        }
                      }
                    case _ =>
                      println ("no start date " + visit)
                  }
              }


              val listBuf2 = if(config.aggregate_by.isDefined)
                config.aggregate_by.get match {
                  case "year" =>
                    def combine(a : JsObject, b:JsObject): JsObject = {
                      b.fields.foldLeft(a)((x, field) =>
                        field match {
                          case (key, value) =>
                            a \ key match {
                              case JsDefined(value0) =>
                                value0 match {
                                  case s:JsString =>
                                    if (value != value0) {
                                      throw new UnsupportedOperationException("string values are different for the same year")
                                    } else
                                      a
                                  case n:JsNumber =>
                                    a + (key -> JsNumber(value.asInstanceOf[JsNumber].value + n.value))
                                  case _ =>
                                    throw new UnsupportedOperationException("unsupport JsValue")
                                }
                              case _ =>
                                a + field
                            }
                        })
                    }
                    listBuf.groupBy { case (start_date, vec) => start_date.withDayOfYear(1) }.mapValues{ g => g.values.fold(Json.obj())(combine) }

                  case _ =>
                    throw new UnsupportedOperationException("aggregate by any field other than year is not implemented")
                }
              else
                listBuf



              val data = listBuf2.toSeq.map {
                case (start_date, vec) =>
                  val age = Years.yearsBetween (birth_date_joda, start_date).getYears
                  val obj = Json.obj (
                    "race_cd" -> race_cd,
                    "sex_cd" -> sex_cd,
                    "birth_date" -> birth_date,
                    "age" -> age,
                    "start_date" -> start_date.toString("y-M-d")) ++ vec
                  if (config.environmental_data.isDefined) {
                    if (config.aggregate_by.isDefined && config.aggregate_by.get != "day") {
                      throw new UnsupportedOperationException("aggregate environmental data is not implemented")
                    }
                    val env = loadEnvData(config, spark, lat, lon, start_date, Seq("o3", "pmij"), Seq("avg", "max", "min", "stddev"))
                    obj ++ env
                  }
                  else
                    obj
              }.filter(crit)

              if (data.nonEmpty) {
                val json = config.output_format match {
                  case "json" =>
                    data.map(obj => Json.stringify (obj)+"\n").mkString("")
                  case "csv" =>
                    val headers = data.map(obj => obj.keys).fold(Set.empty[String])((keys1, keys2) => keys1.union(keys2)).toSeq
                    val rows = data.map(obj => headers.map(col => obj \ col match {
                      case JsDefined(a) =>
                        a.toString
                      case _ =>
                        ""
                    }).mkString("!")).mkString("\n")
                    headers.mkString("!") + "\n" + rows
                  case _ =>
                    throw new UnsupportedOperationException("unsupported output format " + config.output_format)
                }
                writeToFile(hc, output_file, json)
              }

            case _ =>
              println("no birth date " + p)

          }

        }
      }
    }

  case class MDCTN_map_entry (name:Seq[String], rxCUIList:Seq[String], rxCUIList2:Seq[String])
  implicit val MDCTN_map_entry_encoder : org.apache.spark.sql.Encoder[(String, MDCTN_map_entry)] = org.apache.spark.sql.Encoders.kryo[(String, MDCTN_map_entry)]


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("environmental_data").action((x,c) => c.copy(environmental_data = Some(x)))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = Some(DateTime.parse(x))))
      opt[String]("end_date").action((x,c) => c.copy(end_date = Some(DateTime.parse(x))))
      opt[String]("regex_observation").action((x,c) => c.copy(regex_observation = Some(x)))
      opt[String]("regex_observation_filter_visit").action((x,c) => c.copy(regex_observation_filter_visit = Some(x)))
      opt[String]("regex_visit").action((x,c) => c.copy(regex_visit = Some(x)))
      opt[String]("map").action((x,c) => c.copy(map = Some(x)))
      opt[String]("aggregate_by").action((x,c) => c.copy(aggregate_by = Some(x)))
      opt[String]("output_format").action((x,c) => c.copy(output_format = x))
      opt[Unit]("debug").action((_,c) => c.copy(debug = true))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, Config()) match {
      case Some(config) =>

        time {

          case class MDCTN_map(mdctn_rxcui : Map[String, MDCTN_map_entry], rxcui_name : Map[String, String])
          val df = config.map.map(map0 => {
            val df = spark.read.format("csv").option("delimiter", "\t").option("header", false).load(config.input_directory + "/" + map0)
            df.map(row => {
              val row3 = row.getString(3)
              (
                row.getString(0),
                MDCTN_map_entry(row.getString(2).split(";"), row.getString(1).split(";"), if (row3 == null) Seq("[In]") else row3.split(";"))
              )
            }).collect.toMap

          })

          def col_filter_observation(col:String, start_date: DateTime) : (Boolean, Seq[(String, JsValue)]) = {
            df.flatMap(x => x.get(col)).map(map_entry => {
              val tuples = map_entry.rxCUIList.map(rxcuicol => {
                (rxcuicol, JsString(map_entry.rxCUIList2.mkString(";")))
              })
              (false, if(config.debug)
                (col, JsNumber(1)) +: tuples
              else
                tuples)
            }).getOrElse {
              val filtered_visit = config.regex_observation_filter_visit.forall(x => col.matches(x))

              (filtered_visit, config.regex_observation.map(x => {
                if(col.matches(x))
                  Seq((col, JsNumber(1)))
                else
                  Seq.empty
              }).getOrElse(Seq((col, JsNumber(1)))))
            }
          }

          def col_filter_visit(col:String, start_date: DateTime) : Seq[(String, JsValue)] = {
            config.regex_visit.map(x => {
              if(col.matches(config.regex_visit.get))
                Seq((col, JsNumber(1)))
              else
                Seq.empty
            }).getOrElse(Seq((col, JsNumber(1))))
          }

          def crit(jsObject: JsObject) = {
            val start_date = DateTime.parse(jsObject("start_date").as[String], DateTimeFormat.forPattern("Y-M-d"))
            (config.start_date.isEmpty || config.start_date.get.isEqual(start_date) || config.start_date.get.isBefore(start_date)) && (config.end_date.isEmpty || start_date.isBefore(config.end_date.get))
          }

          def proc_pid2(p : String) =
            proc_pid(config, spark, p, col_filter_observation, col_filter_visit, crit)

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid2)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", true).load(config.input_directory + "/" + pdif)

                  val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

                  val count = new AtomicInteger(0)
                  val n = patl.size
                  patl.foreach(pid => {
                    println("processing " + count.incrementAndGet + " / " + n + " " + pid)
                    proc_pid2(pid)
                  })
                case None =>
              }

          }
        }
      case None =>
    }


  spark.stop()


  }
}
