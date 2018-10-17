package datatrans

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.{ ListBuffer, MultiMap }
import scopt._
import datatrans._

case class Config(
  input_directory : String = "",
  output_prefix : String = "",
  start_date : DateTime = DateTime.parse("2010-01-01", ISODateTimeFormat.dateParser()),
  end_date : DateTime = DateTime.parse("2015-01-01", ISODateTimeFormat.dateParser()),
  regex_labs : String = ".*",
  regex_medication : String = ".*",
  regex_condition : String = ".*"
)

object PreprocPerPatSeriesToVector {

  def proc_pid(config : Config, spark: SparkSession, p:String, start_date : DateTime, end_date : DateTime): Unit =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val input_file = f"${config.input_directory}/$p"
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
          import datatrans.Implicits1._
          val pat = loadJson[Patient](hc, new Path(input_file))

          val recs = new ListBuffer[Map[String, Any]]() // a list of encounters, start_time

          val encounter = pat.encounter
          val birth_date_joda = DateTime.parse(pat.birthDate, ISODateTimeFormat.dateParser())
          val sex = pat.gender
          val race = pat.race(0)
          val demographic = Map("patient_num" -> pat, "birth_date" -> birth_date_joda, "sex" -> sex, "race" -> race)
          val inter = new Interval(start_date, end_date)
          encounter.foreach(enc =>{
            var rec = demographic
            val med = enc.medication
            val cond = enc.condition
            val lab = enc.labs
            enc.startDate match {
              case Some(s) =>
                val encounter_start_date_joda = DateTime.parse(s, ISODateTimeFormat.timeParser())
                if (inter.contains(encounter_start_date_joda)) {
                  val age = Years.yearsBetween (birth_date_joda, encounter_start_date_joda).getYears
                  rec += ("start_date" -> encounter_start_date_joda, "age" -> age)

                  med.foreach(m => {
                    if(m.medication.matches(config.regex_medication)) {
                      rec += (m.medication -> 1)
                    }
                  })
                  cond.foreach(m => {
                    if(m.code.matches(config.regex_condition)) {
                      rec += (m.code -> 1)
                    }
                  })
                  lab.foreach(m => {
                    if(m.code.matches(config.regex_labs)) {
                      rec += (m.code -> m.value)
                    }
                  })
                }
                recs.append(rec)
              case None =>
                print("no start_date, skipped " + enc.id)
            }
          })
          val colnames = recs.map(m => m.keySet).fold(Set())((s, s2) => s.union(s2)).toSeq

          val rows = recs.map(m => colnames.map(colname => m.get(colname)))

          import spark.implicits._
          val df = rows.toDF(colnames : _*)
          Utils.writeDataframe(hc, output_file, df) 

        }
      }
    }


  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_prefix").required.action((x,c) => c.copy(output_prefix = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("end_date").action((x,c) => c.copy(end_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("regex_medication").action((x,c) => c.copy(regex_medication = x))
      opt[String]("regex_condition").action((x,c) => c.copy(regex_condition = x))
      opt[String]("regex_labs").action((x,c) => c.copy(regex_labs = x))
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
          val count = new AtomicInteger(0)
          val input_directory_path = new Path(config.input_directory)
          new HDFSCollection(hc, input_directory_path).par.foreach(f => {
            val p = f.getName.split("/")(0)
            println("processing " + count.incrementAndGet + " " + p)
            proc_pid(config, spark, p, start_date_joda, end_date_joda)
          })



        }
      case None =>
    }


  spark.stop()


  }
}
