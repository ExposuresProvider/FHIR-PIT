package datatrans.step

import org.apache.commons.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import scala.collection.mutable.{ ListBuffer, MultiMap }
import scopt._
import scala.collection.JavaConverters._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import io.circe._
import io.circe.generic.semiauto._
import org.apache.log4j.{Logger, Level}

import datatrans.Mapper
import datatrans.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans.Config._
import datatrans._

case class PreprocPerPatSeriesToVectorConfig(
  input_directory : String = null,
  output_directory : String = null,
  start_date : org.joda.time.DateTime = null,
  end_date : org.joda.time.DateTime = null,
  offset_hours: Int = 0,
  feature_map : String = null
)

object PreprocPerPatSeriesToVector extends StepImpl {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  type ConfigType = PreprocPerPatSeriesToVectorConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def proc_pid(config : PreprocPerPatSeriesToVectorConfig, hc : Configuration, p:String, start_date: DateTime, end_date: DateTime, mapper: Mapper): Unit =
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
          val demographic = Map[String, Any]("patient_num" -> pat.id, "birth_date" -> birth_date_joda.toString("yyyy-MM-dd"), "Sex" -> Mapper.map_sex(sex), "Race" -> Mapper.map_race(race), "Ethnicity" -> Mapper.map_ethnicity(ethnicity))
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
                  Some(stringToDateTime(med(0).start))
                } else if(!cond.isEmpty) {
                  // cond encounter use asserted date
                  Some(stringToDateTime(cond(0).assertedDate))
                } else if(!proc.isEmpty) {
                  // cond encounter use asserted date
                  Some(stringToDateTime(proc(0).performedDateTime))
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

          // log.info(f"start_date = $start_date end_date = $end_date start_date.year = ${start_date.year.get} end_date.year = ${end_date.year.get}")
          for (year <- start_date.toDateTime(timeZone).year.get until end_date.toDateTime(timeZone).year.get) {
            recs.append(demographic + ("start_date" -> f"$year-01-01", "VisitType" -> "study"))
          }

          encounter_map.foreach {
            case (encounter_start_date_joda, encset) =>
              var rec0 = demographic
              val age = Years.yearsBetween (birth_date_joda, encounter_start_date_joda).getYears
              rec0 += ("start_date" -> encounter_start_date_joda.toDateTime(timeZone).toString("yyyy-MM-dd"), "AgeVisit" -> age) 

              def incrementName(rec: Map[String, Any], n: String): Map[String, Any] =
                rec.get(n) match {
                  case Some(i) =>
                    rec + (n -> (i.asInstanceOf[Int] + 1))
                  case None =>
                    rec + (n -> 1)
                }

              def setName(rec: Map[String, Any], n: String, v: Any): Map[String, Any] =
                rec + (n -> v)

              def toVector(enc : Encounter) = {
                var rec = rec0 + ("encounter_num" -> enc.id, "VisitType" -> enc.classAttr.map(_.code).getOrElse(""))
                val med = enc.medication
                val cond = enc.condition
                val lab = enc.lab
                val proc = enc.procedure
                val bmi = enc.bmi

                med.foreach(m =>
                  m.coding.foreach(coding => Mapper.map_coding_to_feature(mapper.med_map, coding).foreach(n => {
                    rec = incrementName(rec, n)
                  }))
                )

                cond.foreach(m =>
                  m.coding.foreach(coding => Mapper.map_coding_to_feature(mapper.cond_map, coding).foreach(n => {
                    rec = incrementName(rec, n)
                  }))
                )

                lab.foreach(m =>
                  m.coding.foreach(coding => Mapper.map_coding_to_feature_and_quantity(mapper.obs_map, coding, m).foreach{
                    case (n, v, _) => {
                      rec = setName(rec, n, v)
                    }
                  })
                )

                proc.foreach(m =>
                  m.coding.foreach(coding => Mapper.map_coding_to_feature(mapper.proc_map, coding).foreach(n => {
                    rec = incrementName(rec, n)
                  }))
                )

                rec += ("ObesityBMIVisit" -> (Mapper.map_bmi(bmi) match {
                  case Some(x) => x
                  case None => -1
                }))

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
          deleteCRCFile(output_file_file_system, output_file_path)

        }
      }
    }

  def step(spark: SparkSession, config: PreprocPerPatSeriesToVectorConfig): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {
      val hc = spark.sparkContext.hadoopConfiguration
      val start_date_joda = config.start_date
      val end_date_joda = config.end_date

      val input_directory_path = new Path(config.input_directory)
      val input_directory_file_system = input_directory_path.getFileSystem(hc)

      val mapper = new Mapper(hc, config.feature_map)


      withCounter(count =>
        new HDFSCollection(hc, input_directory_path).par.foreach(f => {
          val p = f.getName
          log.info("processing " + count.incrementAndGet + " " + p)
          proc_pid(config, hc, p, start_date_joda, end_date_joda, mapper)
        })
      )

    }

  }
  
}



