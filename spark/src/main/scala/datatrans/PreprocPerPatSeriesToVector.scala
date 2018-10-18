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
                    if(m.medication.matches(config.regex_medication)) {
                      rec += (m.medication.replaceAll("[/.-]", "_") -> 1)
                    } else {
                      // println(m.medication + " doesn't match " + config.regex_medication)
                    }
                  })
                  cond.foreach(m => {
                    if(m.code.matches(config.regex_condition)) {
                      rec += ("condition_" + m.code.replaceAll("[/.-]", "_") -> 1)
                    } else {
                      // println(m.code + " doesn't match " + config.regex_condition)
                    }
                  })
                  lab.foreach(m => {
                    if(m.code.matches(config.regex_labs)) {
                      rec += ("lab_" + m.code.replaceAll("[/.-]", "_") -> m.value)
                    } else {
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
          new HDFSCollection(hc, input_directory_path).foreach(f => {
            val p = f.getName
            println("processing " + count.incrementAndGet + " " + p)
            proc_pid(config, spark, p, start_date_joda, end_date_joda)
          })
          println("combining output")
          val output_directory_path = new Path(config.output_directory)
          import spark.implicits._
          val dfs = new HDFSCollection(hc, output_directory_path).map(f => {
            println("loading " + f)
            spark.read.format("csv").option("header", value = true).load(f.toString())
          })

          if (!dfs.isEmpty) {
            println("find columns")
            val total = dfs.map(df => df.columns.toSet).reduce((df1, df2) => df1 ++ df2)

            println("extend dataframes")
            def expr(myCols: Seq[String], allCols: Set[String]) = {
              allCols.toList.map(x => x match {
                case x if myCols.contains(x) => col(x)
                case _ => lit(null).as(x)
              })
            }

            val dfs2 = dfs.map(df => df.select(expr(df.columns, total):_*))

            println("combine dataframes")
            val df = dfs2.reduce((df1, df2) => df1.unionAll(df2))
            writeDataframe(hc, config.output_directory + "/all", df)
          }

        }
      case None =>
    }


  spark.stop()


  }
}
