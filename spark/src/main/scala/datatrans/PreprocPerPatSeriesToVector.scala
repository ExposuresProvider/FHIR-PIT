package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.collection.JavaConversions._
import org.joda.time._

import scala.collection.mutable.ListBuffer

object PreprocPerPatSeriesToVector {


  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val cmd = args(0)
      val pdif = args(1)
      val ofif = args(2)
      val col = args(3)
      val base = args(4)
      val output_path = args(5)

      def proc_pid(p:String) =
        time {

          println("processing pid " + p)

          val hc = spark.sparkContext.hadoopConfiguration

          val input_file = base + "/" + p
          val input_file_path = new Path(input_file)
          val input_file_file_system = input_file_path.getFileSystem(hc)

          println("loading json from " + input_file)
          val input_file_input_stream = input_file_file_system.open(input_file_path)

          val jsvalue = Json.parse(input_file_input_stream)
          input_file_input_stream.close()
          println(jsvalue)
          val listBuf = scala.collection.mutable.Map[DateTime, ListBuffer[String]]() // a list of concept, start_time
          val observations = jsvalue("observation").as[JsObject]
          val sex_cd = jsvalue("sex_cd").as[String]
          val race_cd = jsvalue("race_cd").as[String]
          val birth_date = DateTime.parse(jsvalue("birth_date").as[String])

          val encounters = observations.fields
          encounters.foreach{ case (_, encounter) =>
            encounter.as[JsObject].fields.foreach{case (concept_cd, instances) =>
              instances.as[JsObject].fields.foreach{case (_, modifiers) =>
                val start_date = DateTime.parse(modifiers.as[JsObject].values.toSeq(0)("start_date").as[String])
                val age = Days.daysBetween(birth_date, start_date).getDays
                listBuf.get(start_date) match {
                case Some(vec) =>
                  vec.add(concept_cd)

                case None =>
                  val vec = new ListBuffer[String]()
                  listBuf(start_date) = vec
                  vec.add(concept_cd)
                }
              }
            }
          }
          val schema = StructType(Seq(
            StructField("race_cd",StringType),
            StructField("sex_cd",StringType),
            StructField("birth_date", DateType),
            StructField("data", ArrayType(StructType(Seq(
              StructField("age", IntegerType),
              StructField("features", ArrayType(StringType))
            ))))
          ))
          val data = listBuf.toSeq.map{case (start_date, vec) => Row(start_date, vec)}
          val row = Row(race_cd, sex_cd, birth_date, data)
          val ds = spark.createDataFrame(seqAsJavaList(Seq(row)), schema)
          ds.write.json(output_path + p)
        }

      if (cmd == "param") {
        pdif.split(",").foreach(proc_pid)
      } else {
        println("loading patient_dimension from " + pdif)
        val pddf0 = spark.read.format("csv").option("header", true).load(pdif)

        val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

        patl.foreach(proc_pid)
      }


      spark.stop()

    }
  }
}
