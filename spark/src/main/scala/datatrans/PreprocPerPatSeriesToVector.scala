package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scopt._

case class Config(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : String = "",
                   time_series : String = "",
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

  def proc_pid(config : Config, spark: SparkSession, p:String, col_filter_observation: (String, DateTime) => (Boolean, Seq[(String, JsValue)]), col_filter_visit: (String, DateTime) => Seq[(String, JsValue)], crit : JsObject => Boolean): Unit =
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

          val start_date_set = scala.collection.mutable.Set[DateTime]()

          jsvalue \ "birth_date" match {
            case JsDefined (bd) =>
              val birth_date = bd.as[String]
              val birth_date_joda = DateTime.parse (birth_date, DateTimeFormat.forPattern("M/d/y H:m"))
              val sex_cd = jsvalue("sex_cd").as[String]
              val race_cd = jsvalue("race_cd").as[String]
              val demographic = Json.obj(
                "race_cd" -> race_cd,
                "sex_cd" -> sex_cd,
                "birth_date" -> birth_date) ++ extractField(jsvalue, "lat").map(x => x.as[Double]).map(x => Json.obj("lat" -> x)).getOrElse(Json.obj()) ++ extractField(jsvalue, "lon").map(x => x.as[Double]).map(x => Json.obj("lon" -> x)).getOrElse(Json.obj())


              val encounters = observations.fields
              encounters.foreach {
                case (_, encounter) =>
                  encounter.as[JsObject].fields.foreach {
                    case (concept_cd, instances) =>
                      instances.as[JsObject].fields.foreach {
                        case (_, modifiers) =>
                          val start_date = DateTime.parse (modifiers.as[JsObject].values.toSeq.head ("start_date").as[String], DateTimeFormat.forPattern("Y-M-d H:m:s") )
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
                      b.fields.foldLeft(a)((_, field) =>
                        field match {
                          case (key, value) =>
                            a \ key match {
                              case JsDefined(value0) =>
                                value0 match {
                                  case _:JsString =>
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
                    listBuf.groupBy { case (start_date, _) => start_date.withDayOfYear(1) }.mapValues{ g => g.values.fold(Json.obj())(combine) }

                  case _ =>
                    throw new UnsupportedOperationException("aggregate by any field other than year is not implemented")
                }
              else
                listBuf



              val data = listBuf2.toSeq.map {
                case (start_date, vec) =>
                  val age = Years.yearsBetween (birth_date_joda, start_date).getYears
                  demographic ++
                    Json.obj("age" -> age) ++
                    Json.obj("start_date" -> start_date.toString(DATE_FORMAT)) ++ vec
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
                println("writing output to " + output_file)
                writeToFile(hc, output_file, json)
              } else {
                println("no rows found " + p)
              }

            case _ =>
              println("no birth date " + p)

          }

        }
      }
    }


  case class MDCTN_map_entry(name:Seq[String], rxCUIList:Seq[String], rxCUIList2:Seq[String])
  implicit val MDCTN_map_entry_encoder : org.apache.spark.sql.Encoder[(String, MDCTN_map_entry)] = org.apache.spark.sql.Encoders.kryo[(String, MDCTN_map_entry)]


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
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
            println("loading map from " + map0)
            val df = spark.read.format("csv").option("delimiter", "\t").option("header", value = false).load(config.input_directory + "/" + map0)
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
                (rxcuicol, Json.arr(map_entry.rxCUIList2.mkString(";")))
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
              if(col.matches(x))
                Seq((col, JsNumber(1)))
              else
                Seq.empty
            }).getOrElse(Seq((col, JsNumber(1))))
          }

          def crit(jsObject: JsObject) = {
            val start_date = DateTime.parse(jsObject("start_date").as[String], DateTimeFormat.forPattern("Y-M-d"))
            (config.start_date.isEmpty || config.start_date.get.isEqual(start_date) || config.start_date.get.isBefore(start_date)) && (config.end_date.isEmpty || start_date.isBefore(config.end_date.get))
          }

          def proc_pid2(p : String): Unit =
            proc_pid(config, spark, p, col_filter_observation, col_filter_visit, crit)

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid2)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", value = true).load(config.input_directory + "/" + pdif)

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
