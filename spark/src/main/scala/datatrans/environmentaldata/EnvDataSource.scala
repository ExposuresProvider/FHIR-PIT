package datatrans.environmentaldata

import scala.ref.SoftReference
import datatrans.Utils._
import datatrans.components.DataSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import org.joda.time._
import play.api.libs.json.Json.JsValueWrapper

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class LatLon(lat:Double, long: Double)

case class EnvDataSourceConfig(
                                patient_dimension : String = "",
                                time_series : String = "",
                                environmental_data : String = "",
                                output_file : String = "",
                                start_date : DateTime = DateTime.now(),
                                end_date : DateTime = DateTime.now(),
                                output_format : String = "json",
                                geo_coordinates : Boolean = false,
                                sequential : Boolean = false,
                                date_offsets : Seq[Int]= -7 to 7,
                                indices : Seq[String] = Seq("o3", "pm25"),
                                statistics : Seq[String] = Seq("avg", "max", "min", "stddev")
                 )

class EnvDataSource(config: EnvDataSourceConfig) extends DataSource[SparkSession, LatLon, Seq[JsObject]] {
  val cache = TrieMap[String, SoftReference[Seq[DataFrame]]]()

  def loadEnvData(spark: SparkSession, coors: Seq[(Int, (Int, Int))], names: Seq[String]): Map[String, Map[String, Double]] = {

    val dfs = coors.flatMap {
      case (year, (row, col)) =>
        val filename = f"${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"

        def loadEnvDataFrame(filename: String) = {
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

        cache.get(filename) match {
          case None =>
            loadEnvDataFrame(filename)
          case Some(x) =>
            x.get.getOrElse {
              println("SoftReference has already be garbage collected " + filename)
              loadEnvDataFrame(filename)
            }
        }

    }

    if (dfs.nonEmpty) {
      val df = dfs.reduce((a, b) => a.union(b))
      import spark.implicits._
      df.map(row => (row.getString(0), row.getValuesMap[String](names).mapValues(x => x.toDouble))).collect.toMap

    } else
      Map.empty

  }


  def loadDailyEnvData(lat: Double, lon: Double, start_date: DateTime, env_data: Map[String, Map[String, Double]], coors: Map[Int, (Int, Int)], i: Int, names: Seq[String]): JsObject = {
    var env = Json.obj()

    for (ioff <- config.date_offsets) {
      val curr_date = start_date.plusDays(ioff)
      val str = curr_date.toString(DATE_FORMAT)

      env_data.get(str) match {
        case Some(data) =>
          env ++= Json.obj("start_date" -> str)
          env ++= Json.obj(names.flatMap(name => {
            val num = data(name)
            // println("num = " + num)
            if (!num.isNaN)
              Seq(name + "_day" + ioff -> (num: JsValueWrapper))
            else
              Seq()

          }): _*)
        case None =>
      }

      if (config.geo_coordinates) {
        val year = curr_date.year.get
        coors.get(year) match {
          case Some((row, col)) =>
            env ++= Json.obj("row_day" + ioff -> row, "col_day" + ioff -> col, "year_day" + ioff -> year)
          case None =>
        }
      }
    }

    if (config.geo_coordinates)
      env ++= Json.obj(
        "lat" -> lat,
        "lon" -> lon
      )
    env ++= Json.obj("start_date" -> start_date.toString(DATE_FORMAT))

    env
  }

  def get(spark: SparkSession, latlon: LatLon) : Seq[JsObject] =
    latlon match {
      case LatLon(lat, lon) =>

        val hc = spark.sparkContext.hadoopConfiguration

        val data = ListBuffer[JsObject]() // a list of concept, start_time

        val coors = (config.start_date.year.get to config.end_date.minusDays(1).year.get).flatMap(year => {
          latlon2rowcol(lat, lon, year) match {
            case Some((row, col)) =>
              Seq((year, (row, col)))
            case _ =>
              Seq()
          }
        })
        val names = for (i <- config.statistics; j <- config.indices) yield f"${j}_$i"

        val env_data = loadEnvData(spark, coors, names)

        (0 until Days.daysBetween(config.start_date, config.end_date).getDays).map(i =>
          loadDailyEnvData(lat, lon, config.start_date.plusDays(i), env_data, coors.toMap, i, names))

    }



}
