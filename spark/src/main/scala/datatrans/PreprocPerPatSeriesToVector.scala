package datatrans

import scala.ref.SoftReference

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import play.api.libs.json._
import org.joda.time._
import scopt._

import scala.ref.SoftReference
import scala.util.matching.Regex

case class Config(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : String = "",
                   output_prefix : String = "",
                   start_date : Option[DateTime] = None,
                   end_date : Option[DateTime] = None,
                   regex : Option[String] = None,
                   map : Option[String] = None
                 )

object PreprocPerPatSeriesToVector {
  def loadEnvData(config : Config, spark: SparkSession, lat: Double, lon:Double, start_date: DateTime) = {

    var env = Json.obj()

    for(i <- -7 until 7) {
      val start_time = start_date.plusDays(i)

      loadDailyEnvData(config, spark, lat, lon, start_date) match {
        case Some(obj) =>
          env += "o3_avg_day"+i -> obj("o3_avg")
          env += "pmij_avg_day"+i -> obj("pmij_avg")
          env += "o3_max_day"+i -> obj("o3_max")
          env += "pmij_max_day"+i -> obj("pmij_avg")
        case None =>
      }
    }

    env
  }

  val cache = scala.collection.mutable.Map[String, SoftReference[DataFrame]]()

  def loadDailyEnvData(config : Config, spark: SparkSession, lat: Double, lon:Double, start_date: DateTime) : Option[JsObject] = {
    val year = start_date.year.get
    val (row, col) = latlon2rowcol(lat, lon, year)

    if (row == -1 || col == -1) {
      None
    } else {
      val filename = f"${config.input_directory}/cmaq$year/C$col%03dR$row%03dDaily.csv"
      val df = cache.get(filename).flatMap(x => x.get).getOrElse {
        val df = spark.read.format("csv").load(filename).toDF("a","o3_avg","pmij_avg","o3_max","pmij_max")
        cache(filename) = new SoftReference(df)
        df
      }

      val aggregate = df.filter(df("start_date") === start_date.toString("%Y-%m-%D")).select("o3_avg", "pmij_avg", "o3_max", "pmij_max").first
      Some(Json.obj(
          "o3_avg" -> aggregate.getDouble(0),
          "pmij_avg" -> aggregate.getDouble(1),
          "o3_max" -> aggregate.getDouble(2),
          "pmij_max" -> aggregate.getDouble(3)
        ))

    }
  }

  def proc_pid(config : Config, spark: SparkSession, p:String, col_filter: (String, DateTime) => Seq[(String, JsValue)], crit : JsObject => Boolean) =
    time {

      println("processing pid " + p)

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = config.input_directory + "/" + p
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

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

        jsvalue \ "birth_date" match {
          case JsDefined (bd) =>
            val birth_date = bd.as[String]
            val birth_date_joda = DateTime.parse (birth_date)

            val encounters_visit = visits.fields
            encounters_visit.foreach {
              case (visit, encounter) =>
                encounter \ "start_date" match {
                  case JsDefined (x) =>
                    val start_date = DateTime.parse (x.as[String] )
                    encounter \ "inout_cd" match {
                      case JsDefined (y) =>
                        val inout_cd = y.as[String]
                        col_filter(inout_cd, start_date).foreach {
                          case (col, value) =>
                            insertOrUpdate(listBuf, start_date, col, value)
                        }
                      case _ =>
                        println ("no inout cd " + visit)
                    }
                  case _ =>
                    println ("no start date " + visit)
                }
            }

            val encounters = observations.fields
            encounters.foreach {
              case (_, encounter) =>
                encounter.as[JsObject].fields.foreach {
                  case (concept_cd, instances) =>
                    instances.as[JsObject].fields.foreach {
                      case (_, modifiers) =>
                        val start_date = DateTime.parse (modifiers.as[JsObject].values.toSeq (0) ("start_date").as[String] )
                        col_filter(concept_cd, start_date).foreach {
                          case (col, value) =>
                            insertOrUpdate(listBuf, start_date, col, value)
                        }

                    }
                }
            }



            val data = listBuf.toSeq.map {
              case (start_date, vec) =>
                val age = Years.yearsBetween (birth_date_joda, start_date).getYears
                val env = loadEnvData(config, spark, lat, lon, start_date)
                Json.obj (
                  "race_cd" -> race_cd,
                  "sex_cd" -> sex_cd,
                  "birth_date" -> birth_date,
                  "age" -> age,
                  "start_date" -> start_date.toString("%Y-%m-%D")
                ) ++ vec ++ env
            }.filter(crit)

            val json = data.map(obj => Json.stringify (obj)+"\n").mkString("")

            writeToFile(hc, config.output_prefix + p, json)
          case _ =>
            println("no birth date " + p)

        }
      }
    }

  case class MDCTN_map_entry (name:String, rxCUIList:Seq[String], rxCUIList2:Seq[String])
  implicit val MDCTN_map_entry_encoder : org.apache.spark.sql.Encoder[(String, MDCTN_map_entry)] = org.apache.spark.sql.Encoders.kryo[(String, MDCTN_map_entry)]


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = Some(DateTime.parse(x))))
      opt[String]("end_date").action((x,c) => c.copy(end_date = Some(DateTime.parse(x))))
      opt[String]("regex").action((x,c) => c.copy(regex = Some(x)))
      opt[String]("map").action((x,c) => c.copy(map = Some(x)))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, Config()) match {
      case Some(config) =>

        time {

          case class MDCTN_map(mdctn_rxcui : Map[String, MDCTN_map_entry], rxcui_name : Map[String, String])
          val df = config.map.map(map0 => {
            val df = spark.read.format("csv").option("delimiter", "\t").option("header", false).load(config.input_directory + "/" + map0)
            val valmap = df.map(row => {
              val row3 = row.getString(3)
              (
                row.getString(0),
                MDCTN_map_entry(row.getString(2).stripSuffix(";"), row.getString(1).split(";"), if (row3 == null) Seq.empty else row3.split(";"))
              )
            }
              ).collect.toMap
            val colmap = valmap.flatMap {
              case (_, MDCTN_map_entry(name, rxCUIList, _)) =>
                rxCUIList.map(rxCUI => (rxCUI, name))
            }
            MDCTN_map(valmap, colmap)
          })

          def col_filter(col:String, start_date: DateTime) : Seq[(String, JsValue)]= {
            if (df.isDefined && df.get.mdctn_rxcui.contains(col)) {
              val map_entry = (df.get.mdctn_rxcui)(col)
              val colmap = df.get.rxcui_name
              map_entry.rxCUIList.flatMap(col => {
                val colname = colmap(col)
                Seq((colname, JsNumber(1)), (colname + "2", JsString(map_entry.rxCUIList2.mkString(";"))))
              })
            } else if(config.regex.isDefined) {
              if(col.matches(config.regex.get))
                Seq((col, JsNumber(1)))
              else
                Seq.empty

            } else {
              Seq((col, JsNumber(1)))
            }
          }

          def crit(jsObject: JsObject) = {
            val start_date = DateTime.parse(jsObject("start_date").as[String])
            (config.start_date.isEmpty || config.start_date.get.isEqual(start_date) || config.start_date.get.isBefore(start_date)) && (config.end_date.isEmpty || start_date.isBefore(config.end_date.get))
          }

          def proc_pid2(p : String) =
            proc_pid(config, spark, p, col_filter, crit)

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid2)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", true).load(pdif)

                  val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

                  patl.foreach(proc_pid2)
                case None =>
              }

          }
        }
      case None =>
    }


  spark.stop()


  }
}
