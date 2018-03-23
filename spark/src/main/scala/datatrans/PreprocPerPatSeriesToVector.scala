package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import play.api.libs.json._

import org.joda.time._

import scopt._

import scala.util.matching.Regex

case class Config(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : Option[String] = None,
                   output_prefix : Option[String] = None,
                   start_date : Option[DateTime] = None,
                   end_date : Option[DateTime] = None,
                   regex : Option[Regex] = None,
                   map : Option[String] = None
                 )

object PreprocPerPatSeriesToVector {
  def loadEnvData(config : Config, spark: SparkSession, lat: Double, lon:Double, start_date: DateTime) = {
    val year = start_date.year.get
    val (row, col) = latlon2rowcol(lat, lon, year)


    val filename = f"${config.input_directory}/cmaq$year/C$col%03dR$row%03dDaily.csv"
    val df = spark.read.format("csv").load(filename).toDF("a","o3_avg","pmij_avg","o3_max","pmij_max")

    val env = Json.obj()

    for(i <- -7 until 7) {
      val start_time = start_date.plusDays(i)
      val end_time = start_date.plusDays(i + 1)

      val aggregate = df.filter(df("start_date") === start_time.toString("%Y-%m-%D")).select("o3_avg", "pmij_avg", "o3_max", "pmij_max").first
      env("o3_avg_day"+i) = aggregate.getDouble(0)
      env("pmij_avg_day"+i) = aggregate.getDouble(1)
      env("o3_max_day"+i) = aggregate.getDouble(2)
      env("pmij_max_day"+i) = aggregate.getDouble(3)
    }

    env
  }

  def proc_pid(config : Config, spark: SparkSession, p:String, col_filter: (String, DateTime) => Option[(String, JsValue)], crit : JsObject => Boolean) =
    time {

      println("processing pid " + p)

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = config.input_directory.get + "/" + p
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
                        col_filter(inout_cd, start_date) match {
                          case Some((col, value)) =>
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
                        col_filter(concept_cd, start_date) match {
                          case Some((col, value)) =>
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

            writeToFile(hc, config.output_prefix.get + p, json)
          case _ =>
            println("no birth date " + p)

        }
      }
    }


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = Some(x)))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = Some(x)))
      opt[String]("start_date").action((x,c) => c.copy(start_date = Some(DateTime.parse(x))))
      opt[String]("end_date").action((x,c) => c.copy(end_date = Some(DateTime.parse(x))))
      opt[String]("regex").action((x,c) => c.copy(regex = Some(x.r)))
      opt[String]("map").action((x,c) => c.copy(map = Some(x)))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, Config()) match {
      case Some(config) =>

        time {
          val df = if(config.map.isDefined)
            Some(spark.read.format("csv").option("header", true).load(config.input_directory + "/mdctn_rxnorm_map.csv")
              .map(row => (row.getString(0), (row.getString(1), JsString(row.getString(2))))).collect.toMap)
          else
            None


          def col_filter(col:String, start_date: DateTime) : Option[(String, JsValue)]= {
            if (df.isDefined && df.get.contains(col)) {
              Some(df.get(col))
            } else if(config.regex.isDefined) {
              col match {
                case config.regex.get =>
                  Some((col, JsNumber(1)))
                case _ =>
                  None
              }
            } else {
              Some((col, JsNumber(1)))
            }
          }

          def crit(jsObject: JsObject) = {
            val start_date = DateTime.parse(jsObject("start_date").as[String])
            (config.start_date.isEmpty || config.start_date.get <= start_date) && (config.end_date.isEmpty || start_date < config.end_date.get)
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
