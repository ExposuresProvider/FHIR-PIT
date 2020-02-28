package datatrans.environmentaldata

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession, Column, Row}
import org.apache.spark.sql.types._
import org.joda.time._
import datatrans.step.EnvDataConfig

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.log4j.{Logger, Level}
import datatrans.environmentaldata.Utils._

class EnvDataSource(spark: SparkSession, config: EnvDataConfig) {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)
  val envSchema = StructType(
    StructField("start_date", DateType) ::
      (for(j <- Seq("avg", "max", "min", "stddev"); i <- Seq("o3", "pm25")) yield i + "_" + j).map(i => StructField(i, DoubleType)).toList
  )

  def loadEnvDataFrame(input: (String, Seq[String], StructType)) : Option[DataFrame] = {
    val (filename, names, schema) = input
    val df = readCSV(spark, filename, schema, (_: String) => DoubleType)
    if (names.forall(x => df.columns.contains(x))) {
      Some(df.cache())
    } else {
      log.error(f"$filename doesn't contain all required columns")
      val namesNotInDf = names.filter(x => !df.columns.contains(x))
      Some(namesNotInDf.foldLeft(df)((df : DataFrame, x : String) => df.withColumn(x, lit(null))))
    }
  }
    

  def loadRowColDataFrame(coors: Seq[(Int, (Int, Int))]) : DataFrame = {
    val dfs = coors.flatMap {
      case (year, (row, col)) =>
        val filename = f"${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"
        loadEnvDataFrameCache((filename, names, envSchema))
    }
    if (dfs.nonEmpty) {
      dfs.reduce((a, b) => a.union(b)).cache()
    } else {
      log.error(f"input env df is not available ${coors}")
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], envSchema)
    }
  }

  def generateOutputDataFrame(key: (Seq[(Int, (Int, Int))], Seq[Int])) = {
    val (coors, years) = key
    val df = loadRowColDataFrameCache(coors)

    val dfyear = aggregateByYear(spark, df, names, config.statistics, Seq())
   
    def stats(names2 : Seq[String]) = names2 ++ (for (i <- config.statistics; j <- names2) yield f"${j}_$i")

    dfyear.select("start_date", stats(names): _*).cache()
  }

  private val loadEnvDataFrameCache = new Cache(loadEnvDataFrame)
  private val loadRowColDataFrameCache = new Cache(loadRowColDataFrame)
  private val generateOutputDataFrameCache = new Cache(generateOutputDataFrame)
  val names = for (i <- config.statistics; j <- config.indices) yield f"${j}_$i"

  def run(): Unit = {

    time {
      import spark.implicits._

      val patient_dimension = config.patgeo_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)

      val patl = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList

      val hc = spark.sparkContext.hadoopConfiguration

      val n = patl.size

      withCounter(count => 

      patl.par.foreach{
        case (r, lat, lon) =>
          log.info("processing patient " + count.incrementAndGet() + " / " + n + " " + r)
          val output_file = config.output_file.replace("%i", r)
          val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
          val start_date_local = config.start_date.toDateTime(timeZone)
          val end_date_local = config.end_date.toDateTime(timeZone).minusDays(1)
          val yearseq = start_date_local.year.get to end_date_local.year.get
          log.info(f"loading env data from year sequence $yearseq, start_date = ${start_date_local}, end_date = ${end_date_local}")
          val coors = yearseq.intersect(Seq(2010,2011)).flatMap(year => {
            latlon2rowcol(lat, lon, year) match {
              case Some((row, col)) =>
                Seq((year, (row, col)))
              case _ =>
                Seq()
            }
          })

          val df = generateOutputDataFrameCache((coors, yearseq))
          writeDataframe(hc, output_file, df)

      })
    }
  }
}
