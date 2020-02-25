package datatrans.step

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.types._
import org.joda.time._

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.log4j.{Logger, Level}

import net.jcazevedo.moultingyaml._

import datatrans.environmentaldata._
import datatrans.environmentaldata.Utils._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._


case class EnvDataSourceFIPSConfig(
  patgeo_data : String,
  environmental_data : String,
  output_file : String,
  start_date : DateTime,
  end_date : DateTime,
  fips_data: String,
  statistics : Seq[String], // = Seq("avg", "max"),
  indices : Seq[String], // = Seq("ozone_daily_8hour_maximum", "pm25_daily_average")
  offset_hours : Int
) extends StepConfig

object PreprocPerPatSeriesEnvDataFIPSYamlProtocol extends SharedYamlProtocol {
  implicit val preprocPerPatSeriesEnvDataFIPSYamlFormat = yamlFormat9(EnvDataSourceFIPSConfig)
}

object PreprocPerPatSeriesEnvDataFIPS extends StepConfigConfig {

  type ConfigType = EnvDataSourceFIPSConfig

  val yamlFormat = PreprocPerPatSeriesEnvDataFIPSYamlProtocol.preprocPerPatSeriesEnvDataFIPSYamlFormat

  val configType = classOf[EnvDataSourceFIPSConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  val FIPSSchema = StructType(
    List(
      StructField("Date", StringType),
      StructField("FIPS", StringType),
      StructField("Longitude", StringType),
      StructField("Latitude", StringType)
    )
  )

  def loadEnvDataFrame(spark: SparkSession, filename: String, names: Seq[String], schema: StructType) : Option[DataFrame] = {
    val hc = spark.sparkContext.hadoopConfiguration
    val path = new Path(filename)
    val file_system = path.getFileSystem(hc)
    if(file_system.isFile(path)) {
      val df = readCSV(spark, filename, schema, (_: String) => DoubleType)
      Some(
        if (names.forall(x => df.columns.contains(x))) {
          df
        } else {
          log.error(f"$filename doesn't contain all required columns")
          val namesNotInDf = names.filter(x => !df.columns.contains(x))
          namesNotInDf.foldLeft(df)((df : DataFrame, x : String) => df.withColumn(x, lit(null)))
        })
    } else {
      None
    }
  }

  def union(a: DataFrame, b: DataFrame): DataFrame = {
    val acols = a.columns.toSet
    val bcols = b.columns.toSet
    val cols = acols ++ bcols
    val a2 = cols -- acols
    val b2 = cols -- bcols
    val a3 = a2.foldLeft(a)((df: DataFrame, col:String) => df.withColumn(col, lit(null)))
    val b3 = b2.foldLeft(b)((df: DataFrame, col:String) => df.withColumn(col, lit(null)))
    a3.union(b3)
  }

  def loadFIPSDataFrame(spark: SparkSession, config: EnvDataSourceFIPSConfig, years: Seq[Int]) : Option[DataFrame] = {
    val dfs2 = years.flatMap(year => {
      val filename = f"${config.environmental_data}/merged_cmaq_$year.csv"
      loadEnvDataFrame(spark, filename, config.indices, FIPSSchema)
    })
    if(dfs2.nonEmpty) {
      val df2 = dfs2.reduce((a, b) => union(a, b))
      val df3 = df2.withColumn("start_date", to_date(df2.col("Date"),"yy/MM/dd"))
      Some(df3)
    } else {
      None
    }
  }

  def step(spark: SparkSession, config: EnvDataSourceFIPSConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.patgeo_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
      val patl = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList
      val n = patl.size
      val geoidfinder = new GeoidFinder(config.fips_data, "")
      log.info("generating geoid")
      val patl_geoid = patl.par.map {
        case (r, lat, lon) =>
          (r, geoidfinder.getGeoidForLatLon(lat, lon))
      }.seq
      val pat_geoid = pddf0.select("patient_num", "lat", "lon").join(patl_geoid.toDF("patient_num", "FIPS"), Seq("patient_num"), "left")

      val hc = spark.sparkContext.hadoopConfiguration

      val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
      val start_date_local = config.start_date.toDateTime(timeZone)
      val end_date_local = config.end_date.toDateTime(timeZone).minusDays(1)
      val yearseq = start_date_local.year.get to end_date_local.year.get

      val indices = config.indices
      val names = for (i <- config.statistics; j <- indices) yield f"${j}_$i"

      log.info("loading env data")
      val df3yearm = loadFIPSDataFrame(spark, config, yearseq) match {
        case Some(df3) =>
          log.info(f"columns = ${df3.columns.toSeq}")
          val df3aggbyyear = aggregateByYear(spark, pat_geoid.join(df3, Seq("FIPS"), "left").withColumn("year", year(df3.col("start_date"))), indices, Seq("FIPS"))
          val names3 = for ((_, i) <- yearlyStatsToCompute; j <- indices) yield f"${j}_$i"
          log.info(f"columns with aggregation = ${df3aggbyyear.columns.toSeq}")
          val df3year = df3aggbyyear.select("start_date", indices ++ names3 ++ indices.map((s: String) => f"${s}_prev_date"): _*)


          withCounter(count =>

            patl.par.foreach{
              case (r, _, _) =>
                log.info("processing patient " + count.incrementAndGet() + " / " + n + " " + r)
                val output_file = config.output_file.replace("%i", r)

                writeDataframe(hc, output_file, df3year.filter($"patient_num" === r))

            }
          )
        case None =>
          log.error(f"input fips df is not available ${yearseq}")
      }

    }
  }

}
