package datatrans.environmentaldata

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import scala.ref.SoftReference
import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import org.joda.time._
import play.api.libs.json.Json.JsValueWrapper

import scala.collection.concurrent.TrieMap
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

case class EnvDataSourceConfig(
  patgeo_data : String = "",
  environmental_data : String = "",
  output_file : String = "",
  start_date : DateTime = DateTime.now(),
  end_date : DateTime = DateTime.now(),
  fips_data: String = "",
  indices : Seq[String] = Seq("o3", "pm25"),
  statistics : Seq[String] = Seq("avg", "max"),
  indices2 : Seq[String] = Seq("ozone_daily_8hour_maximum", "pm25_daily_average")
)

class EnvDataSource(config: EnvDataSourceConfig) {
  private val cache = TrieMap[String, SoftReference[Seq[DataFrame]]]()
  val geoidfinder = new GeoidFinder(config.fips_data, "")

  def loadEnvData(spark: SparkSession, coors: Seq[(Int, (Int, Int))], fips: String, years: Seq[Int], names: Seq[String], fipsnames: Seq[String]) = {

    def loadEnvDataFrame2(filename: String, names : Seq[String]) = {
      val df = spark.read.format("csv").option("header", value = true).load(filename)
      if (names.forall(x => df.columns.contains(x))) {
        cache(filename) = new SoftReference(Seq(df))
        println("SoftReference created for " + filename)
        Seq(df)
      } else {
        print(f"$filename doesn't contain all required columns")
        Seq()
      }
    }

    def loadEnvDataFrame(filename: String, names: Seq[String]) = {
      this.synchronized {
        cache.get(filename) match {
          case None =>
            loadEnvDataFrame2(filename, names)
          case Some(x) =>
            x.get.getOrElse {
              println("SoftReference has already be garbage collected " + filename)
              loadEnvDataFrame2(filename, names)
            }
        }
      }
    }

    val dfs = coors.flatMap {
      case (year, (row, col)) =>
        val filename = f"${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"
        loadEnvDataFrame(filename, names)
    }

    val dfs2 = years.flatMap(year => {
      val filename = f"${config.environmental_data}/merged_cmaq_$year.csv"
      val df = loadEnvDataFrame(filename, fipsnames)
      df.map(df => df.filter(df.col("FIPS") === fips))
    })


    if (dfs.nonEmpty && dfs2.nonEmpty) {
      val df = dfs.reduce((a, b) => a.union(b))
      val df2 = dfs2.reduce((a, b) => a.union(b))
      val df3 = df2.withColumn("start_date", to_date(df2.col("Date"),"yy/MM/dd")).drop("Date")
      Some(df.join(df3, Seq("start_date"), "outer"))

    } else {
      None
    }

  }


  def get(spark: SparkSession, lat: Double, lon: Double) : Option[DataFrame] = {
    val yearseq = (config.start_date.year.get to config.end_date.minusDays(1).year.get)
    val coors = yearseq.intersect(Seq(2010,2011)).flatMap(year => {
      latlon2rowcol(lat, lon, year) match {
        case Some((row, col)) =>
          Seq((year, (row, col)))
        case _ =>
          Seq()
      }
    })
    val fips = geoidfinder.getGeoidForLatLon(lat, lon)
    val names = for (i <- config.statistics; j <- config.indices) yield f"${j}_$i"

    loadEnvData(spark, coors, fips, yearseq, names, config.indices2)

  }

  def run(spark: SparkSession): Unit = {

    import spark.implicits._

    val patient_dimension = config.patgeo_data
    println("loading patient_dimension from " + patient_dimension)
    val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)

    val patl = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList

    val hc = spark.sparkContext.hadoopConfiguration

    val count = new AtomicInteger(0)
    val n = patl.size

    patl.foreach {
      case (r, lat, lon) =>
        println("processing " + count.incrementAndGet + " / " + n + " " + r)

        val output_file = config.output_file.replace("%i", r)
        val output_file_path = new Path(output_file)
        val output_file_file_system = output_file_path.getFileSystem(hc)
        if(output_file_file_system.exists(output_file_path)) {
          println(output_file + " exists")
        } else {
          get(spark, lat, lon) match {
            case Some(df) =>
              writeDataframe(hc, output_file, df)
            case None =>
          }
        }
    }



  }


}
