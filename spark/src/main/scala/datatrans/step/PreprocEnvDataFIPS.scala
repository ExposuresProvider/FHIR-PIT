package datatrans.step

import scala.collection.JavaConverters._

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils.{time, readCSV, writeDataframe}
import org.apache.spark.sql.{DataFrame, SparkSession, Column, Row}                           
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, DateType}
import org.joda.time.{DateTime, DateTimeZone}

import org.apache.spark.sql.functions.{lit, to_date, year, broadcast}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Logger, Level}

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import datatrans.StepImpl
import datatrans.Mapper


case class EnvDataFIPSConfig(
  environmental_data : String,
  output_file : String,
  start_date : DateTime,
  end_date : DateTime,
  fips_data: String,
  offset_hours : Int
)

object PreprocEnvDataFIPS extends StepImpl {

  type ConfigType = EnvDataFIPSConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

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
          val namesNotInDf = names.filter(x => !df.columns.contains(x))
          log.error(f"$filename doesn't contain all required columns adding columns $namesNotInDf")
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
    a3.unionByName(b3)
  }

  def loadFIPSDataFrame(spark: SparkSession, config: EnvDataFIPSConfig, years: Seq[Int]) : Option[DataFrame] = {
    val dfs2 = years.flatMap(year => {
      val filename = f"${config.environmental_data}/merged_cmaq_$year.csv"
      loadEnvDataFrame(spark, filename, Mapper.envInputColumns2, FIPSSchema)
    })
    if(dfs2.nonEmpty) {
      val df2 = dfs2.reduce((a, b) => union(a, b))
      val df3 = df2.withColumn("start_date", to_date(df2.col("Date"),"yy/MM/dd"))
      Some(df3)
    } else {
      None
    }
  }

  def step(spark: SparkSession, config: EnvDataFIPSConfig) = {
    time {
      log.info("starting EnvDataFIPS")
      import spark.implicits._

      val patient_dimension = config.fips_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
      val pat_geoid = pddf0.select("patient_num", "FIPS")
      val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList
      val n = patl.size

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file_all = config.output_file

      val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
      val start_date_local = config.start_date.toDateTime(timeZone)
      val end_date_local = config.end_date.toDateTime(timeZone).minusDays(1)
      val yearseq = start_date_local.year.get to end_date_local.year.get

      log.info("loading env data")
      val df3 = loadFIPSDataFrame(spark, config, yearseq) match {
        case Some(df3) =>
          log.info(f"columns1 = ${df3.columns.toSeq}, nrows1 = ${df3.count()}")
          df3
        case None =>
          log.error(f"input fips df is not available ${yearseq}")
          val schema = StructType(Seq(
            StructField("Date", StringType, true),
            StructField("FIPS", StringType, true),
            StructField("Longitude", DoubleType, true),
            StructField("Latitude", DoubleType, true),
            StructField("CO_ppbv", DoubleType, true),
            StructField("NO_ppbv", DoubleType, true),
            StructField("NO2_ppbv", DoubleType, true),
            StructField("NOX_ppbv", DoubleType, true),
            StructField("SO2_ppbv", DoubleType, true),
            StructField("ALD2_ppbv", DoubleType, true),
            StructField("FORM_ppbv", DoubleType, true),
            StructField("pm25_daily_average", DoubleType, true),
            StructField("pm25_daily_average_stderr", DoubleType, true),
            StructField("ozone_daily_8hour_maximum", DoubleType, true),
            StructField("ozone_daily_8hour_maximum_stderr", DoubleType, true),
            StructField("start_date", DateType, true)
          ))
          spark.createDataFrame(Seq[Row]().asJava, schema)
      }

      val df3year = df3.withColumn("year", year($"start_date"))
      //        df3year.cache()
      log.info(f"columns2 = ${df3year.columns.toSeq}, nrows1 = ${df3year.count()}")

      val df3year_pat = df3.join(broadcast(pat_geoid), Seq("FIPS"), "inner")
      //        df3year_pat.cache()
      log.info(f"columns3 = ${df3year_pat.columns.toSeq}, nrows1 = ${df3year_pat.count()}")

      writeDataframe(hc, output_file_all, df3year_pat)

    }
  }

}












