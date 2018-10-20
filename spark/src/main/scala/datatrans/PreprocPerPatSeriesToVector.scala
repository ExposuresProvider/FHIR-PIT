package datatrans

import org.apache.commons.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ SparkSession, Column, Row }
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.{ ListBuffer, MultiMap }
import scopt._
import datatrans._
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

case class Config(
  input_directory : String = "",
  output_directory : String = "",
  start_date : DateTime = DateTime.parse("2010-01-01", ISODateTimeFormat.dateParser()),
  end_date : DateTime = DateTime.parse("2015-01-01", ISODateTimeFormat.dateParser()),
  med_map : Option[String] = None
)

object PreprocPerPatSeriesToVector {
  val asthmare = "(493[.]|J45[.]).*".r
  val croupre = "(464[.]|J05[.]).*".r
  val reactiveAirwayRe = "(496[.]|J44[.]|J66[.]).*".r
  val coughRe = "(786[.]|R05[.]).*".r
  val pneumoniaRe = "(48[1-6][.]|J1[2-8].).*".r
  val obesityRe = "(278[.]|E66.[^3]).*".r
  def map_condition(code : String) : Seq[String] = {
    println("mapping condition code " + code)
    code match {
      case asthmare(_*) =>
        Seq("AsthmaDx")
      case croupre(_*) =>
        Seq("CroupDx")
      case reactiveAirwayRe(_*) =>
        Seq("ReactiveAirwayDx")
      case coughRe(_*) =>
        Seq("CoughDx")
      case pneumoniaRe(_*) =>
        Seq("PneumoniaDx")
      case obesityRe(_*) =>
        Seq("ObesityDx")
      case _ =>
        Seq()
    }
  }

  def map_labs(code : String) : Seq[String] = {
    code match {
      case _ =>
        Seq()
    }
  }
  def map_procedure(system : String, code : String) : Seq[String] = Seq() /* {
    system match {
      case "http://www.ama-assn.org/go/cpt/" =>
        code match {
          case "94010" =>
            Seq("spirometry")
          case "94070" =>
            Seq("multiple spirometry")
          case "95070" =>
            Seq("methacholine challenge test")
          case "94620" =>
            Seq("simple exercise stress test")
          case "94621" =>
            Seq("complex exercise stress test")
          case "31624" =>
            Seq("bronchoscopy")
          case "94375" =>
            Seq("flow-volume loop")
          case "94060" =>
            Seq("spirometry (pre/post bronchodilator test)")
          case "94070" =>
            Seq("bronchospasm provocation")
          case "95070" =>
            Seq("inhalation bronchial challenge")
          case "94664" =>
            Seq("bronchodilator administration")
          case "94620" =>
            Seq("pulmonary stress test")
          case "95027" =>
            Seq("airborne allergen panel")
          case _ =>
            Seq()
        }
      case _ =>
        Seq()
    }
  } */

  def map_medication(medmap : Option[Map[String, String]], code : String) : Seq[String] = {
    medmap match {
      case Some(mm) =>
        mm.get(code.stripPrefix("Medication/")) match {
          case Some(ms) =>
            ms.split(";").map(m => m.stripSuffix("[IN]").trim().capitalize).filter(m => m.nonEmpty)
          case None =>
            Seq()
        }
      case None =>
        Seq(code.replace("[/|-:]", "_"))
    }
  }


  def proc_pid(config : Config, hc : Configuration, p:String, start_date : DateTime, end_date : DateTime, medmap : Option[Map[String, String]]): Unit =
    time {



      val input_file = f"${config.input_directory}/$p"
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

      val output_file = config.output_directory + "/" + p
      val output_file_path = new Path(output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        println(output_file + " exists")
      } else {

        if(!input_file_file_system.exists(input_file_path)) {
          println("json not found, skipped " + p)
        } else {
          println("loading json from " + input_file)
          import datatrans.Implicits2._
          val pat = loadJson[Patient](hc, new Path(input_file))

          val recs = new ListBuffer[Map[String, Any]]() // a list of encounters, start_time

          val encounter = pat.encounter
          val birth_date_joda = DateTime.parse(pat.birthDate, ISODateTimeFormat.dateParser())
          val sex = pat.gender
          val race = pat.race.filter(r => r != "UNMAPPED").mkString("|")
          val demographic = Map[String, Any]("patient_num" -> pat.id, "birth_date" -> birth_date_joda.toString("yyyy-MM-dd"), "sex" -> sex, "race" -> race)
          val intv = new Interval(start_date, end_date)

          val encounter_map = new scala.collection.mutable.HashMap[DateTime, scala.collection.mutable.Set[Encounter]] with MultiMap[DateTime, Encounter]
          encounter.foreach(enc => {
            (enc.startDate match {
              case Some(s) =>
                Some(DateTime.parse(s, ISODateTimeFormat.dateTimeParser()))
              case None =>
                val med = enc.medication
                val cond = enc.condition
                val lab = enc.labs
                val proc = enc.procedure
                if(!med.isEmpty) {
                  // med encounter use authorized on date
                  Some(DateTime.parse(med(0).authoredOn, ISODateTimeFormat.dateTimeParser()))
                } else if(!cond.isEmpty) {
                  // cond encounter use asserted date
                  Some(DateTime.parse(cond(0).assertedDate, ISODateTimeFormat.dateTimeParser()))
                } else if(!proc.isEmpty) {
                  // cond encounter use asserted date
                  Some(DateTime.parse(proc(0).performedDateTime, ISODateTimeFormat.dateTimeParser()))
                } else {
                  if(!med.isEmpty || !cond.isEmpty || !lab.isEmpty || !proc.isEmpty) {
                    // throw new RuntimeException("non empty encountner has no start date " + p + " " + enc.id)
                  }
                  None
                }
            }).foreach(encounter_start_date_joda => {
              if (intv.contains(encounter_start_date_joda)) {
                encounter_map.addBinding(encounter_start_date_joda, enc)
              }
            })
          })

          encounter_map.foreach {
            case (encounter_start_date_joda, encset) =>
              var rec0 = demographic
              val age = Years.yearsBetween (birth_date_joda, encounter_start_date_joda).getYears
              rec0 += ("start_date" -> encounter_start_date_joda.toString("yyyy-MM-dd"), "age" -> age) 

              def toVector(enc : Encounter) = {
                var rec = rec0 + ("encounter_num" -> enc.id, "encounter_code" -> enc.code.getOrElse(""))
                val med = enc.medication
                val cond = enc.condition
                val lab = enc.labs
                val proc = enc.procedure

                med.foreach(m => {
                  map_medication(medmap, m.medication).foreach(n => {
                    rec += (n -> 1)
                  })
                })
                cond.foreach(m => {
                  map_condition(m.code).foreach(n => {
                    rec += (n -> 1)
                  })
                })
                lab.foreach(m => {
                  map_labs(m.code).foreach(n => {
                    rec += (n -> m.value)
                  })
                })
                proc.foreach(m => {
                  map_procedure(m.system, m.code).foreach(n => {
                    rec += (n -> 1)
                  })
                })
                recs.append(rec)
              }

              def mergeEncounter(a: Encounter, b: Encounter): Encounter = {
                val code = a.code match {
                  case Some(ac) =>
                    b.code match {
                      case Some(bc) =>
                        Some((ac.split("[|]").toSet ++ bc.split("[|]").toSet).mkString("|"))
                      case None =>
                        a.code
                    }
                  case None =>
                    b.code
                }
                val startDate = a.startDate match {
                  case Some(ac) =>
                    a.startDate
                  case None =>
                    b.startDate
                }
                val endDate = a.endDate match {
                  case Some(ac) =>
                    a.endDate
                  case None =>
                    b.endDate
                }

                Encounter(a.id + "|" + b.id, a.subjectReference, code, startDate, endDate, a.condition ++ b.condition, a.labs ++ b.labs, a.medication ++ b.medication, a.procedure ++ b.procedure)

              }

              if(encset.size > 1) {
                println("merge encounters " + p + " " + encset.map(enc => enc.id))
              }
              toVector(encset.reduce(mergeEncounter))

          }

          val colnames = recs.map(m => m.keySet).fold(Set())((s, s2) => s.union(s2)).toSeq
          val output_file_csv_writer = new CSVPrinter( new OutputStreamWriter(output_file_file_system.create(output_file_path), "UTF-8" ) , CSVFormat.DEFAULT.withHeader(colnames:_*))
          
          recs.foreach(m => {
            output_file_csv_writer.printRecord(colnames.map(colname => m.get(colname).getOrElse("")).asJava)
          })
          output_file_csv_writer.close()

        }
      }
    }

  def loadMedMap(hc : Configuration, med_map : String) : Map[String, String] = {
    val med_map_path = new Path(med_map)
    val input_directory_file_system = med_map_path.getFileSystem(hc)

    val csvParser = new CSVParser(new InputStreamReader(input_directory_file_system.open(med_map_path), "UTF-8"), CSVFormat.DEFAULT
      .withDelimiter('\t')
      .withTrim())

    try {
      Map(csvParser.asScala.map(rec => (rec.get(0).stripPrefix("MDCTN:"), rec.get(2).stripSuffix(";"))).toSeq : _*)
    } finally {
      csvParser.close()
    }
   
  }
  
  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_directory").required.action((x,c) => c.copy(output_directory = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("end_date").action((x,c) => c.copy(end_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("mdctn_rxnorm").action((x,c) => c.copy(med_map = Some(x)))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    parser.parse(args, Config()) match {
      case Some(config) =>

        time {
          val hc = spark.sparkContext.hadoopConfiguration
          val start_date_joda = config.start_date
          val end_date_joda = config.end_date

          val input_directory_path = new Path(config.input_directory)
          val input_directory_file_system = input_directory_path.getFileSystem(hc)

          val medmap = config.med_map.map(med_map => loadMedMap(hc, med_map))


          withCounter(count =>
            new HDFSCollection(hc, input_directory_path).foreach(f => {
              val p = f.getName
              println("processing " + count.incrementAndGet + " " + p)
              proc_pid(config, hc, p, start_date_joda, end_date_joda, medmap)
            })
          )

        }
      case None =>
    }


    spark.stop()


  }
}
