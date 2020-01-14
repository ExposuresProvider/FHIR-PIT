package datatrans.step

import org.apache.commons.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import scala.collection.mutable.{ ListBuffer, MultiMap }
import scopt._
import scala.collection.JavaConverters._
import squants.mass.{Kilograms, Grams, Pounds}
import squants.space.{Centimeters, Inches}
import datatrans.Config._
import net.jcazevedo.moultingyaml._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import io.circe._
import org.apache.log4j.{Logger, Level}

import datatrans.Implicits._
import datatrans.Config._
import datatrans._

case class PreprocPerPatSeriesToVectorConfig(
  input_directory : String,
  output_directory : String,
  start_date : org.joda.time.DateTime,
  end_date : org.joda.time.DateTime,
  offset_hours: Int,
  med_map : String
) extends StepConfig

object PreprocPerPatSeriesToVectorYamlProtocol extends SharedYamlProtocol {
  implicit val preprocPerPatSeriesToVectorYamlFormat = yamlFormat6(PreprocPerPatSeriesToVectorConfig)
}

object PreprocPerPatSeriesToVector extends StepConfigConfig {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)
  type ConfigType = PreprocPerPatSeriesToVectorConfig

  val yamlFormat = PreprocPerPatSeriesToVectorYamlProtocol.preprocPerPatSeriesToVectorYamlFormat

  val configType = classOf[PreprocPerPatSeriesToVectorConfig].getName()

  def map_condition(coding: Coding) : Seq[String] =
    ConditionMapper.map_condition(coding.system, coding.code)

  def sort_by_effectiveDateTime(lab : Seq[Lab]) : Seq[Lab] =
    lab.sortWith((a, b) => {
      val at = DateTime.parse(a.effectiveDateTime, ISODateTimeFormat.dateTimeParser())
      val bt = DateTime.parse(b.effectiveDateTime, ISODateTimeFormat.dateTimeParser())
      if(at == bt && a.value != b.value) {
        log.info("warning: two labs in one encounter has same effectiveDateTime but different values")
      }
      at.isBefore(bt)
    })

  def map_lab(lab : Seq[Lab]) : Seq[(String, Any)] = {
    def filter_by_code(code : String) =
      sort_by_effectiveDateTime(lab.filter(lab => lab.coding.exists((x) => x.code == code)))
    val wbc = filter_by_code("6690-2") // 26464-8
    val hct = filter_by_code("20570-8") // 24360-0
    val plt = filter_by_code("26515-7") // 7773
    val fev1 = filter_by_code("20150-9") // 52485-0
    val fvc = filter_by_code("19870-5") // 52485-0
    val fev1fvc = filter_by_code("19926-5") // 52485-0
    val listBuf = new ListBuffer[(String, Any)]()

    def extractValue(lab: Lab) = lab.value.map(x => x.asInstanceOf[ValueQuantity].valueNumber).getOrElse(null)
    def extractFlag(lab: Lab) = lab.flag.getOrElse(null)
    def extractColumns(lab: Seq[Lab], prefix: String) = {
      if(!lab.isEmpty) {
        Seq(
          (f"${prefix}_FirstValue", extractValue(lab.head)),
          (f"${prefix}_FirstFlag", extractFlag(lab.head)),
          (f"${prefix}_LastValue", extractValue(lab.last)),
          (f"${prefix}_LastFlag", extractFlag(lab.last))
        )
      } else {
        Seq()
      }
    }

    def extractColumns2(lab: Seq[Lab], prefix: String) = {
      if(!lab.isEmpty) {
        Seq(
          (f"${prefix}_FirstValue", extractValue(lab.head)),
          (f"${prefix}_LastValue", extractValue(lab.last))
        )
      } else {
        Seq()
      }
    }

    extractColumns(wbc, "WBC") ++
    extractColumns(hct, "HCT") ++
    extractColumns(plt, "PLT") ++
    extractColumns(fev1fvc, "FEV1FVC") ++
    extractColumns2(fev1, "FEV1") ++
    extractColumns2(fvc, "FVC")

  }

  def map_procedure(coding : Coding) : Seq[String] = Seq() /* {
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

  def map_medication(medmap : Map[String, String], coding : Coding) : Option[String] =
    medmap.get(coding.code) match {
      case Some(ms) =>
        val medfiltered = meds.find(med => ms.toLowerCase == med.toLowerCase)
        // log.info("medication " + ms + " " + meds + " " + medfiltered)
        medfiltered
      case None =>
        log.info("cannot find medication name for code " + coding.code)
        None
    }

  def map_race(race : Seq[String]) : String =
    if(race.isEmpty) {
      "Unknown"
    } else {
      race.head.trim match {
        case "2106-3" => "Caucasian"
        case "2054-5" => "African American"
        case "2028-9" => "Asian"
        case "2076-8" => "Native Hawaiian/Pacific Islander"
        case "1002-5" => "American/Alaskan Native"
        case _ => "Other(" + race.head + ")"
      }
    }

  def map_ethnicity(ethnicity : Seq[String]) : String =
    if(ethnicity.isEmpty) {
      "Unknown"
    } else {
      ethnicity.head match {
        case "2135-2" => "Hispanic"
        case "2186-5" => "Not Hispanic"
        case _ => "Unknown"
      }
    }

  def map_sex(sex : String) : String =
    sex match {
      case "male" => "Male"
      case "female" => "Female"
      case _ => "Unknown"
    }

  def map_bmi(bmi : Seq[Lab]) : Option[Double] = {
    def filter_by_code(code : String) =
      bmi.filter(bmi => bmi.coding.exists((x) => x.code == code))
    val bmiQuas = filter_by_code(LOINC.BMI)
    val heightQuas = filter_by_code(LOINC.BODY_HEIGHT)
    val weightQuas = filter_by_code(LOINC.BODY_WEIGHT)
    bmiQuas match {
      case Seq() =>
        (heightQuas, weightQuas) match {
          case (heightQ :: _ , weightQ :: _) =>
            val heightQua = heightQ.value.asInstanceOf[ValueQuantity]
            val weightQua = weightQ.value.asInstanceOf[ValueQuantity]
            val heightVal = heightQua.valueNumber
            val heightUnit = heightQua.unit
            val weightVal = weightQua.valueNumber
            val weightUnit = weightQua.unit
            val height = (heightUnit match {
              case Some("in") =>
                Inches
              case Some("[in_i]") =>
                Inches
              case Some("cm") =>
                Centimeters
              case _ =>
                throw new RuntimeException("unsupported unit " + heightUnit)
            })(heightVal) to Inches
            val weight = (weightUnit match {
              case Some("lbs") =>
                Pounds
              case Some("[lb_av]") =>
                Pounds
              case Some("kg") =>
                Kilograms
              case Some("g") =>
                Grams
              case _ =>
                throw new RuntimeException("unsupported unit " + weightUnit)
            })(weightVal) to Pounds
            Some(weight / math.pow(height, 2) * 703)
          case _ =>
            None
        }
      case bmiQua :: _ =>
        Some(bmiQua.value.asInstanceOf[ValueQuantity].valueNumber)
    }
  }

  def proc_pid(config : PreprocPerPatSeriesToVectorConfig, hc : Configuration, p:String, start_date : DateTime, end_date : DateTime, medmap : Map[String, String]): Unit =
    time {



      val input_file = f"${config.input_directory}/$p"
      val input_file_path = new Path(input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)

      val output_file = config.output_directory + "/" + p + ".csv"
      val output_file_path = new Path(output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        log.info(output_file + " exists")
      } else {

        if(!input_file_file_system.exists(input_file_path)) {
          log.info("json not found, skipped " + p)
        } else {
          log.info("loading json from " + input_file)
          val pat = loadJson[Patient](hc, new Path(input_file))

          val recs = new ListBuffer[Map[String, Any]]() // a list of encounters, start_time

          val encounter = pat.encounter
          val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
          val birth_date_joda = LocalDateTime.parse(pat.birthDate, ISODateTimeFormat.dateParser()).toDateTime(timeZone)
          val sex = pat.gender
          val race = pat.race
          val ethnicity = pat.ethnicity
          val demographic = Map[String, Any]("patient_num" -> pat.id, "birth_date" -> birth_date_joda.toString("yyyy-MM-dd"), "Sex" -> map_sex(sex), "Race" -> map_race(race), "Ethnicity" -> map_ethnicity(ethnicity))
          val intv = new Interval(start_date, end_date)

          val encounter_map = new scala.collection.mutable.HashMap[DateTime, scala.collection.mutable.Set[Encounter]] with MultiMap[DateTime, Encounter]
          encounter.foreach(enc => {
            (enc.startDate match {
              case Some(s) =>
                Some(DateTime.parse(s, ISODateTimeFormat.dateTimeParser()))
              case None =>
                val med = enc.medication
                val cond = enc.condition
                val lab = enc.lab
                val proc = enc.procedure
                val bmi = enc.bmi
                if(!med.isEmpty) {
                  // med encounter use start date
                  Some(DateTime.parse(med(0).start, ISODateTimeFormat.dateTimeParser()))
                } else if(!cond.isEmpty) {
                  // cond encounter use asserted date
                  Some(DateTime.parse(cond(0).assertedDate, ISODateTimeFormat.dateTimeParser()))
                } else if(!proc.isEmpty) {
                  // cond encounter use asserted date
                  Some(DateTime.parse(proc(0).performedDateTime, ISODateTimeFormat.dateTimeParser()))
                } else {
                  if(!med.isEmpty || !cond.isEmpty || !lab.isEmpty || !proc.isEmpty || !bmi.isEmpty) {
                    log.info("non empty encountner has no start date " + p + " " + enc.id)
                  }
                  None
                }
            }).foreach(encounter_start_date_joda => {
              if (intv.contains(encounter_start_date_joda)) {
                encounter_map.addBinding(encounter_start_date_joda, enc)
              }
            })
          })

          pat.medication.foreach(med => {
            val medication_authoredOn_joda = DateTime.parse(med.start, ISODateTimeFormat.dateTimeParser())
            if (intv.contains(medication_authoredOn_joda)) {
              encounter_map.addBinding(medication_authoredOn_joda, Encounter("", "", None, None, None, Seq(), Seq(), Seq(med), Seq(), Seq()))
            }
          })

          pat.condition.foreach(cond => {
            val condition_assertedDate_joda = DateTime.parse(cond.assertedDate, ISODateTimeFormat.dateTimeParser())
            if (intv.contains(condition_assertedDate_joda)) {
              encounter_map.addBinding(condition_assertedDate_joda, Encounter("", "", None, None, None, Seq(cond), Seq(), Seq(), Seq(), Seq()))
            }
          })

          pat.lab.foreach(cond => {
            val condition_assertedDate_joda = DateTime.parse(cond.effectiveDateTime, ISODateTimeFormat.dateTimeParser())
            if (intv.contains(condition_assertedDate_joda)) {
              encounter_map.addBinding(condition_assertedDate_joda, Encounter("", "", None, None, None, Seq(), Seq(cond), Seq(), Seq(), Seq()))
            }
          })

          pat.procedure.foreach(cond => {
            val condition_assertedDate_joda = DateTime.parse(cond.performedDateTime, ISODateTimeFormat.dateTimeParser())
            if (intv.contains(condition_assertedDate_joda)) {
              encounter_map.addBinding(condition_assertedDate_joda, Encounter("", "", None, None, None, Seq(), Seq(), Seq(), Seq(cond), Seq()))
            }
          })

          pat.bmi.foreach(b => {
            throw new RuntimeException("error: " + b)
          })

          encounter_map.foreach {
            case (encounter_start_date_joda, encset) =>
              var rec0 = demographic
              val age = Years.yearsBetween (birth_date_joda, encounter_start_date_joda).getYears
              rec0 += ("start_date" -> encounter_start_date_joda.toDateTime(timeZone).toString("yyyy-MM-dd"), "AgeVisit" -> age) 

              def toVector(enc : Encounter) = {
                var rec = rec0 + ("encounter_num" -> enc.id, "VisitType" -> enc.classAttr.map(_.code).getOrElse(""))
                val med = enc.medication
                val cond = enc.condition
                val lab = enc.lab
                val proc = enc.procedure
                val bmi = enc.bmi

                med.foreach(m =>
                  m.coding.foreach(coding => map_medication(medmap, coding).foreach(n => {
                    rec += (n -> 1)
                  }))
                )
                cond.foreach(m =>
                  m.coding.foreach(coding => map_condition(coding).foreach(n => {
                    rec += (n -> 1)
                  }))
                )
                map_lab(lab).foreach(rec += _)

                rec += ("ObesityBMIVisit" -> (map_bmi(bmi) match {
                  case Some(x) => x
                  case None => -1
                }))

                proc.foreach(m =>
                  m.coding.foreach(coding => map_procedure(coding).foreach(n => {
                    rec += (n -> 1)
                  }))
                )
                recs.append(rec)
              }

              def mergeSetString(a: String, b: String): String =
                (a.split("[|]").toSet ++ b.split("[|]").toSet).toSeq.sorted.mkString("|")

              def mergeOption[T](a:Option[T], b:Option[T], f:(T,T)=>T): Option[T] =
                a match {
                  case Some(ac) =>
                    b match {
                      case Some(bc) =>
                        Some(f(ac,bc))
                      case None =>
                        a
                    }
                  case None =>
                    b
                }

              def mergeEncounter(a: Encounter, b: Encounter): Encounter = {
                val classAttr = mergeOption(a.classAttr, b.classAttr, (ac:Coding, bc:Coding) => Coding(mergeSetString(ac.system, bc.system), mergeSetString(ac.code, bc.code), mergeOption(ac.display, bc.display, mergeSetString)))
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

                Encounter(a.id + "|" + b.id, a.subjectReference, classAttr, startDate, endDate, a.condition ++ b.condition, a.lab ++ b.lab, a.medication ++ b.medication, a.procedure ++ b.procedure, a.bmi ++ b.bmi)

              }

              if(encset.size > 1) {
                log.info("merge encounters " + p + " " + encset.map(enc => enc.id))
              }
              toVector(encset.reduce(mergeEncounter))

          }

          val colnames = recs.map(m => m.keySet).fold(Set())((s, s2) => s.union(s2)).toSeq
          val output_file_csv_writer = new CSVPrinter( new OutputStreamWriter(output_file_file_system.create(output_file_path), "UTF-8" ) , CSVFormat.DEFAULT.withHeader(colnames:_*))
          
          recs.foreach(m => {
            val row = colnames.map(colname => m.get(colname).getOrElse(""))
            output_file_csv_writer.printRecord(row.asJava)
          })
          output_file_csv_writer.close()

        }
      }
    }

  def loadMedMap(hc : Configuration, med_map : String) : Map[String, String] = {
    val med_map_path = new Path(med_map)
    val input_directory_file_system = med_map_path.getFileSystem(hc)

    val json = Utils.parseInputStream(input_directory_file_system.open(med_map_path))
    Utils.decode[Map[String, String]](json)

  }

  def step(spark: SparkSession, config: PreprocPerPatSeriesToVectorConfig): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    import DefaultYamlProtocol._

    time {
      val hc = spark.sparkContext.hadoopConfiguration
      val start_date_joda = config.start_date
      val end_date_joda = config.end_date

      val input_directory_path = new Path(config.input_directory)
      val input_directory_file_system = input_directory_path.getFileSystem(hc)

      val medmap = loadMedMap(hc, config.med_map)


      withCounter(count =>
        new HDFSCollection(hc, input_directory_path).foreach(f => {
          val p = f.getName
          log.info("processing " + count.incrementAndGet + " " + p)
          proc_pid(config, hc, p, start_date_joda, end_date_joda, medmap)
        })
      )

    }

  }
  
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import PreprocPerPatSeriesToVectorYamlProtocol._

    parseInput[PreprocPerPatSeriesToVectorConfig](args) match {
      case Some(config) =>
        step(spark, config)
      case None =>
    }


    spark.stop()


  }
}

object LOINC {
  val BMI = "39156-5"
  val BODY_HEIGHT = "8302-2"
  val BODY_WEIGHT = "29463-7"
}

