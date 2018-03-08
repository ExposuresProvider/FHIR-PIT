package datatrans

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import play.api.libs.json._
import scala.collection.JavaConversions._

import scala.collection.mutable.ListBuffer

object PreprocPerPatSeriesToVector {


  def main(args: Array[String]) {
    time {
      val spark = SparkSession.builder().appName("datatrans preproc").config("spark.sql.pivotMaxValues", 100000).config("spark.executor.memory", "16g").config("spark.driver.memory", "64g").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val pdif = args(0)
      val ofif = args(1)
      val col = args(2)
      val base = args(3)
      val output_file = args(4)
      val output_path = args(5)

      val df = spark.read.format("csv").option("header", true).load(ofif)
      val odf = df.select(col).distinct.map(r => r.getString(0))

      odf.write.csv(output_file)

      val cols = odf.collect.toSeq.sorted

      println("loading column namespatient_dimension from " + pdif)
      val pddf0 = spark.read.format("csv").option("header", true).load(pdif)

      val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList.par

      patl.foreach{p =>
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
          val listBuf = scala.collection.mutable.Map[String, Array[Int]]() // a list of concept, start_time
          val observations = jsvalue("observation").as[JsObject]
          val encounters = observations.fields
          encounters.foreach{ case (_, encounter) =>
            encounter.as[JsObject].fields.foreach{case (concept_cd, instances) =>
              val col_index = cols.indexOf(concept_cd)
              instances.as[JsObject].fields.foreach{case (_, modifiers) =>
                val start_date = modifiers.as[JsObject].values.toSeq(0)("start_date").as[String]
                listBuf.get(start_date) match {
                case Some(vec) =>
                  vec(col_index) = 1

                case None =>
                  val vec = Array.fill[Int](cols.length)(0)
                  vec(col_index) = 1
                  listBuf(start_date) = vec

                }
              }
            }
          }
          val schema = StructType(Seq(StructField("start_date", StringType), StructField("features", ArrayType(IntegerType))))
          val rows = listBuf.toSeq.map{case (start_date, vec) => Row(start_date, vec)}
          val ds = spark.createDataFrame(seqAsJavaList(rows), schema).sort("start_date")
          ds.write.csv(output_path + p)
        }
      }

      spark.stop()

    }
  }
}
