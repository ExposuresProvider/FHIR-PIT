package datatrans

import java.sql.Date
import java.time.ZoneId

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.collection.JavaConversions._
import org.joda.time._

import scala.collection.mutable.ListBuffer
import scopt._

case class Config(
                   patient_dimension : Option[String] = None,
                   patient_num_list : Option[Seq[String]] = None,
                   input_directory : Option[String] = None,
                   output_prefix : Option[String] = None
                 )

object PreprocPerPatSeriesToVector {


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = Some(x)))
      opt[Seq[String]]("patient_num_list").action((x,c) => c.copy(patient_num_list = Some(x)))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = Some(x)))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = Some(x)))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, Config()) match {
      case Some(config) =>

        time {

          def proc_pid(p:String) =
            time {

              println("processing pid " + p)

              val hc = spark.sparkContext.hadoopConfiguration

              val input_file = config.input_directory.get + "/" + p
              val input_file_path = new Path(input_file)
              val input_file_file_system = input_file_path.getFileSystem(hc)

              println("loading json from " + input_file)
              val input_file_input_stream = input_file_file_system.open(input_file_path)

              val jsvalue = Json.parse(input_file_input_stream)
              input_file_input_stream.close()
              val listBuf = scala.collection.mutable.Map[Int, ListBuffer[String]]() // a list of concept, start_time

              val visits = jsvalue("visit").as[JsObject]
              val observations = jsvalue("observation").as[JsObject]
              val sex_cd = jsvalue("sex_cd").as[String]
              val race_cd = jsvalue("race_cd").as[String]

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
                          val age = Days.daysBetween (birth_date_joda, start_date).getDays
                          encounter \ "inout_cd" match {
                            case JsDefined (y) =>
                              val inout_cd = y.as[String]
                              listBuf.get (age) match {
                                case Some (vec) =>
                                  vec.add (inout_cd)

                                case None =>
                                  val vec = new ListBuffer[String] ()
                                  listBuf (age) = vec
                                  vec.add (inout_cd)
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
                              val age = Days.daysBetween (birth_date_joda, start_date).getDays
                              listBuf.get (age) match {
                                case Some (vec) =>
                                  vec.add (concept_cd)

                                case None =>
                                  val vec = new ListBuffer[String] ()
                                  listBuf (age) = vec
                                  vec.add (concept_cd)
                              }
                          }
                      }
                  }

                  val data = listBuf.toSeq.map {
                    case (age, vec) => Json.obj (
                      "age" -> age,
                      "features" -> vec
                    )
                  }.sortBy (row => row ("age").as[Int] )

                  val o = Json.obj (
                    "race_cd" -> race_cd,
                    "sex_cd" -> sex_cd,
                    "birth_date" -> birth_date,
                    "data" -> Json.arr (data)
                  )


                  val json = Json.stringify (o)

                  writeToFile(hc, config.output_prefix.get + p, json)
                case _ =>
                  println("no birth date " + p)

              }

            }

          config.patient_num_list match {
            case Some(pnl) =>
              pnl.par.foreach(proc_pid)
            case None =>
              config.patient_dimension match {
                case Some(pdif) =>
                  println("loading patient_dimension from " + pdif)
                  val pddf0 = spark.read.format("csv").option("header", true).load(pdif)

                  val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

                  patl.foreach(proc_pid)
                case None =>
              }

          }
        }
      case None =>
    }

    spark.stop()


  }
}
