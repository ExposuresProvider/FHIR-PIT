package datatrans.environmentaldata

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils.{time, withCounter, writeDataframe, readCSV, latlon2rowcol}
import org.apache.spark.sql.{DataFrame, SparkSession, Column, Row}
import org.apache.spark.sql.types.{StructType, StructField, DateType, DoubleType}
import org.joda.time.DateTimeZone
import datatrans.step.EnvDataCoordinatesConfig

import org.apache.spark.sql.functions.year
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Logger, Level}
import datatrans.Mapper

class EnvDataSource(spark: SparkSession, config: EnvDataCoordinatesConfig) {
  val log = Logger.getLogger(getClass.getName)
  val hc = spark.sparkContext.hadoopConfiguration

  log.setLevel(Level.INFO)
  val envSchema = StructType(
    StructField("start_date", DateType) ::
      (for(j <- Seq("avg", "max", "min", "stddev"); i <- Seq("o3", "pm25")) yield i + "_" + j).map(i => StructField(i, DoubleType)).toList
  )

  def loadEnvDataFrame(filename: String, names: Seq[String], schema: StructType) : Option[DataFrame] = {
    val filepath = new Path(filename)
    val file_file_system = filepath.getFileSystem(hc)
    if (file_file_system.exists(filepath)) {
      val df = readCSV(spark, filename, schema, (_: String) => DoubleType)
      if (names.forall(x => df.columns.contains(x))) {
        Some(df)
      } else {
        log.error(f"$filename doesn't contain all required columns")
        None
      }
    } else {
      None
    }
  }
    

  def generateOutputDataFrame(coors: Seq[(Int, (Int, Int))]) : DataFrame = {
    val dfs = coors.flatMap {
      case (year, (row, col)) =>
        val filename = f"${config.environmental_data}/cmaq$year/C$col%03dR$row%03dDaily.csv"
        loadEnvDataFrame(filename, Mapper.envInputColumns, envSchema)
    }
    if (dfs.nonEmpty) {
      dfs.reduce((a, b) => a.unionByName(b))
    } else {
      log.error(f"input env df is not available ${coors}")
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], envSchema)
    }
  }

  def run(): Unit = {

    time {
      import spark.implicits._

      val patient_dimension = config.patgeo_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)

      val patl = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList


      val n = patl.size

      withCounter(count => 

        patl.par.foreach{
          case (r, lat, lon) =>
            log.info("processing patient " + count.incrementAndGet() + " / " + n + " " + r)
            val timeZone = DateTimeZone.forOffsetHours(config.offset_hours)
            val start_date_local = config.start_date.toDateTime(timeZone)
            val end_date_local = config.end_date.toDateTime(timeZone).minusDays(1)
            val yearseq = start_date_local.year.get to end_date_local.year.get
            val output_file = f"${config.output_dir}/$r"
            val output_file_path = new Path(output_file)
            val output_file_file_system = output_file_path.getFileSystem(hc)
            if (!output_file_file_system.exists(output_file_path)) {
              log.info(f"loading env data from year sequence $yearseq, start_date = ${start_date_local}, end_date = ${end_date_local}")
              val coors = yearseq.intersect(Seq(2010,2011)).flatMap(year => {
                latlon2rowcol(lat, lon, year) match {
                  case Some((row, col)) =>
                    Seq((year, (row, col)))
                  case _ =>
                    Seq()
                }
              })
              
              val df = generateOutputDataFrame(coors)
              writeDataframe(hc, output_file, df)
            }
        })
    }
  }
}
