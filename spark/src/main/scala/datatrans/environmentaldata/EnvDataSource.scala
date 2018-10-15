package datatrans.environmentaldata

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import org.joda.time._
import play.api.libs.json.Json.JsValueWrapper

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


class EnvDataSource(spark: SparkSession, config: EnvDataSourceConfig) {
  def loadEnvDataFrame(input: (String, Seq[String])) = {
    val (filename, names) = input
    val df = spark.read.format("csv").option("header", value = true).load(filename)
    if (names.forall(x => df.columns.contains(x))) {
      Some(df)
    } else {
      print(f"$filename doesn't contain all required columns")
      None
    }
  }

  def loadFIPSDataFrame(input: (String, String, Seq[String])) = {
    val (filename, fips, names) = input
    val df = inputCache((filename, config.indices2))
    df.map(df => df.filter(df.col("FIPS") === fips))
  }

  def generateOutputDataFrame(key: (Seq[(Int, (Int, Int))], String, Seq[Int])) = {
    val (coors, fips, years) = key


    val dfs = coors.flatMap {
      case (year, (row, col)) =>
        val filename = f"${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"
        inputCache((filename, names))

    }

    val dfs2 = years.flatMap(year => {
      val filename = f"${config.environmental_data}/merged_cmaq_$year.csv"
      inputCache2((filename, fips, config.indices2))
    })


    if (dfs.nonEmpty && dfs2.nonEmpty) {
      val df = dfs.reduce((a, b) => a.union(b))
      val df2 = dfs2.reduce((a, b) => a.union(b))
      val df3 = df2.withColumn("start_date", to_date(df2.col("Date"),"yy/MM/dd"))
      Some(df.join(df3, Seq("start_date"), "outer").select("start_date", names ++ config.indices2 : _*))

    } else {
      None
    }
  }

  private val inputCache = new Cache(loadEnvDataFrame)
  private val inputCache2 = new Cache(loadFIPSDataFrame)
  private val outputCache = new Cache(generateOutputDataFrame)
  val names = for (i <- config.statistics; j <- config.indices) yield f"${j}_$i"

  val geoidfinder = new GeoidFinder(config.fips_data, "")

  def get(lat: Double, lon: Double) : Option[DataFrame] = {
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

    outputCache((coors, fips, yearseq))

  }

  def run(): Unit = {

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
          get(lat, lon) match {
            case Some(df) =>
              writeDataframe(hc, output_file, df)
            case None =>
          }
        }
    }

  }


}
