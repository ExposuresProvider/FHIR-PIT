package datatrans.step

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils.{time, writeDataframe}
import org.apache.spark.sql.{DataFrame, SparkSession, Column}

import org.apache.log4j.{Logger, Level}

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import datatrans.StepImpl


case class LatLonToGeoidConfig(
  patgeo_data : String,
  output_file : String,
  fips_data: String
)

object PreprocLatLonToGeoid extends StepImpl {

  type ConfigType = LatLonToGeoidConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder
  
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: LatLonToGeoidConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.patgeo_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
      val patl = pddf0.select("patient_num", "lat", "lon").map(r => (r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble)).collect.toList

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file = config.output_file

      val geoidfinder = new GeoidFinder(config.fips_data, "")
      log.info("generating geoid")
      val patl_geoid = patl.par.map {
        case (r, lat, lon) =>
          (r, geoidfinder.getGeoidForLatLon(lat, lon))
      }.seq
      log.info("computing geoids")
      val pat_geoid = pddf0.select("patient_num", "lat", "lon").join(patl_geoid.toDF("patient_num", "FIPS"), Seq("patient_num"), "left")

      writeDataframe(hc, config.output_file, pat_geoid)


    }
  }

}












