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
  environmental_data : String,
  output_file : String,
  start_date : DateTime,
  end_date : DateTime,
  fips_data: String,
  statistics : Seq[String], // = Seq("avg", "max"),
  indices : Seq[String], // = Seq("ozone_daily_8hour_maximum", "pm25_daily_average")
  offset_hours : Int
) extends StepConfig

object PreprocEnvDataFIPSYamlProtocol extends SharedYamlProtocol {
  implicit val preprocEnvDataFIPSYamlFormat = yamlFormat8(EnvDataSourceFIPSConfig)
}

object PreprocEnvDataFIPS extends StepConfigConfig {

  type ConfigType = EnvDataSourceFIPSConfig

  val yamlFormat = PreprocEnvDataFIPSYamlProtocol.preprocEnvDataFIPSYamlFormat

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

      val patient_dimension = config.fips_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
      val pat_geoid = pddf0.select("patient_num", "FIPS")
      val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList
      val n = patl.size

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file_all = config.output_file
      val output_file_all_path = new Path(output_file_all)
      val output_file_all_file_system = output_file_all_path.getFileSystem(hc)

      val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
      val start_date_local = config.start_date.toDateTime(timeZone)
      val end_date_local = config.end_date.toDateTime(timeZone).minusDays(1)
      val yearseq = start_date_local.year.get to end_date_local.year.get

      val indices = config.indices
      val names = for (i <- config.statistics; j <- indices) yield f"${j}_$i"

      log.info("loading env data")
      loadFIPSDataFrame(spark, config, yearseq) match {
        case Some(df3) =>
          log.info(f"columns1 = ${df3.columns.toSeq}, nrows1 = ${df3.count()}")

          val df3year = df3.withColumn("year", year($"start_date"))
          //        df3year.cache()
          log.info(f"columns2 = ${df3year.columns.toSeq}, nrows1 = ${df3year.count()}")

          val df3year_pat = pat_geoid.join(df3, Seq("FIPS"), "inner")
          //        df3year_pat.cache()
          log.info(f"columns3 = ${df3year_pat.columns.toSeq}, nrows1 = ${df3year_pat.count()}")

          log.info(f"aggregating $indices")
          val df3year_pat_aggbyyear = aggregateByYear(spark, df3year_pat, indices, Seq("FIPS"))
          //        df3year_pat_aggbyyear.cache()
          // log.info(f"columns4 = ${df3year_pat_aggbyyear.columns.toSeq}, nrows1 = ${df3year_pat_aggbyyear.count()}")

          val names3 = for ((_, i) <- yearlyStatsToCompute; j <- indices) yield f"${j}_$i"
          val df4 = df3year_pat_aggbyyear.select("patient_num", ("start_date" +: indices) ++ names3 ++ indices.map((s: String) => f"${s}_prev_date"): _*)
          // log.info(f"columns5 = ${df4.columns.toSeq}, nrows1 = ${df4.count()}")

          writeDataframe(hc, output_file_all, df4)
        case None =>
          log.error(f"input fips df is not available ${yearseq}")
      }

    }
  }

}












