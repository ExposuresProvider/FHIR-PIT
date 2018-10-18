package datatrans

import org.apache.commons.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
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
  end_date : DateTime = DateTime.parse("2015-01-01", ISODateTimeFormat.dateParser())
)

object PreprocPerPatSeriesToVector {
  val asthmare = "(493[.]|J45[.]).*".r
  val croupre = "(464[.]|J05[.]).*".r
  val reactiveAirwayRe = "(496[.]|J44[.]|J66[.]).*".r
  val coughRe = "(786[.]|R05[.]).*".r
  val pneumoniaRe = "(48[1-6][.]|J1[2-8].).*".r
  val obesityRe = "(278[.]|E66.[^3]).*".r
  def map_condition(code : String) : Option[String] = {
    code match {
      case asthmare(_*) =>
        Some("AsthmaDx")
      case croupre(_*) =>
        Some("CroupDx")
      case reactiveAirwayRe(_*) =>
        Some("ReactiveAirwayDx")
      case coughRe(_*) =>
        Some("CoughDx")
      case pneumoniaRe(_*) =>
        Some("PneumoniaDx")
      case obesityRe(_*) =>
        Some("ObesityDx")
      case _ =>
        None
    }
  }

  def map_labs(code : String) : Option[String] = {
    code match {
      case _ =>
        None
    }
  }
  def map_medication(code : String) : Option[String] = {
    code match {
      case _ =>
        None
    }
  }


  def proc_pid(config : Config, spark: SparkSession, p:String, start_date : DateTime, end_date : DateTime): Unit =
    time {

      val hc = spark.sparkContext.hadoopConfiguration

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
          import spark.implicits._
          val pat = loadJson[Patient](hc, new Path(input_file))

          val recs = new ListBuffer[Map[String, Any]]() // a list of encounters, start_time

          val encounter = pat.encounter
          val birth_date_joda = DateTime.parse(pat.birthDate, ISODateTimeFormat.dateParser())
          val sex = pat.gender
          val race = pat.race(0)
          val demographic = Map[String, Any]("patient_num" -> pat.id, "birth_date" -> birth_date_joda.toString("yyyy-MM-dd"), "sex" -> sex, "race" -> race)
          val intv = new Interval(start_date, end_date)
          encounter.foreach(enc =>{
            var rec = demographic
            val med = enc.medication
            val cond = enc.condition
            val lab = enc.labs
            enc.startDate match {
              case Some(s) =>
                val encounter_start_date_joda = DateTime.parse(s, ISODateTimeFormat.dateTimeParser())
                if (intv.contains(encounter_start_date_joda)) {
                  val age = Years.yearsBetween (birth_date_joda, encounter_start_date_joda).getYears
                  rec += ("start_date" -> encounter_start_date_joda.toString("yyyy-MM-dd"), "age" -> age, "encounter_num" -> enc.id, "encounter_code" -> enc.code.getOrElse(""))

                  med.foreach(m => {
                    map_medication(m.medication) match {
                      case Some(n) =>
                        rec += (n -> 1)
                      case _ =>
                        // println(m.medication + " doesn't match " + config.regex_medication)
                    }
                  })
                  cond.foreach(m => {
                    map_condition(m.code) match {
                      case Some(n) =>
                        rec += (n -> 1)
                      case _ =>
                        // println(m.code + " doesn't match " + config.regex_condition)
                    }
                  })
                  lab.foreach(m => {
                    map_labs(m.code) match {
                      case Some(a) =>
                        rec += (a -> m.value)
                      case _ =>
                        // println(m.code + " doesn't match " + config.regex_labs)
                    }
                  })
                  recs.append(rec)
                }
              case None =>
                // println("no start_date, skipped " + enc.id)
            }
          })
          val colnames = recs.map(m => m.keySet).fold(Set())((s, s2) => s.union(s2)).toSeq
          val output_file_csv_writer = new CSVPrinter( new OutputStreamWriter(output_file_file_system.create(output_file_path), "UTF-8" ) , CSVFormat.DEFAULT.withHeader(colnames:_*))
          
          recs.foreach(m => {
            output_file_csv_writer.printRecord(colnames.map(colname => m.get(colname).getOrElse("")).asJava)
          })
          output_file_csv_writer.close()

        }
      }
    }

  // https://stackoverflow.com/questions/39758045/how-to-perform-union-on-two-dataframes-with-different-amounts-of-columns-in-spar
  def unionDf(df1:org.apache.spark.sql.DataFrame, df2:org.apache.spark.sql.DataFrame) = {
    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val total = cols1 ++ cols2 // union

  }

  def main(args: Array[String]) {
    val parser = new OptionParser[Config]("series_to_vector") {
      head("series_to_vector")
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("output_directory").required.action((x,c) => c.copy(output_directory = x))
      opt[String]("start_date").action((x,c) => c.copy(start_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
      opt[String]("end_date").action((x,c) => c.copy(end_date = DateTime.parse(x, ISODateTimeFormat.dateParser())))
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

          withCounter(count =>
            new HDFSCollection(hc, input_directory_path).foreach(f => {
              val p = f.getName
              println("processing " + count.incrementAndGet + " " + p)
              proc_pid(config, spark, p, start_date_joda, end_date_joda)
            })
          )

          println("combining output")
          val output_directory_path = new Path(config.output_directory)
          val output_directory_file_system = output_directory_path.getFileSystem(hc)
          import spark.implicits._

          val files = new HDFSCollection(hc, output_directory_path).toSeq
          if (!files.isEmpty) {
            println("find columns")
            val total = withCounter(count =>
              files.map(f => {
                println("loading " + count.incrementAndGet + " " + f)
                val csvParser = new CSVParser(new InputStreamReader(output_directory_file_system.open(f), "UTF-8"), CSVFormat.DEFAULT
                  .withFirstRecordAsHeader()
                  .withIgnoreHeaderCase()
                  .withTrim())
                val cos = csvParser.getHeaderMap().keySet().asScala
                csvParser.close()
                cos
              })
            ).reduce((df1, df2) => df1 ++ df2).toSeq

            println("extend dataframes")

            val colnames = withCounter(count =>
              files.map(f => {
                println("loading " + count.incrementAndGet + " " + f)
                val csvParser = new CSVParser(new InputStreamReader(output_directory_file_system.open(f), "UTF-8"), CSVFormat.DEFAULT
                  .withFirstRecordAsHeader()
                  .withIgnoreHeaderCase()
                  .withTrim())
                val keyset = csvParser.getHeaderMap().keySet().asScala
                csvParser.close()
                keyset
              })
            ).reduce((df1, df2) => df1 ++ df2).toSeq

            val output_file_path = new Path(config.output_directory + "/all")
            val output_file_csv_writer = new CSVPrinter( new OutputStreamWriter(output_directory_file_system.create(output_file_path), "UTF-8" ) , CSVFormat.DEFAULT.withHeader(colnames:_*))
          
            withCounter(count =>
              files.foreach(f => {
                println("loading " + count.incrementAndGet + " " + f)
                val csvParser = new CSVParser(new InputStreamReader(output_directory_file_system.open(f), "UTF-8"), CSVFormat.DEFAULT
                  .withFirstRecordAsHeader()
                  .withIgnoreHeaderCase()
                  .withTrim())
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
                csvParser.close()
              })
            )

            output_file_csv_writer.close()
          }


        }
      case None =>
    }


    spark.stop()


  }
}
