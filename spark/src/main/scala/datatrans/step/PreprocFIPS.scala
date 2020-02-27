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


case class FIPSConfig(
  patgeo_data : String,
  output_file : String,
  fips_data: String
) extends StepConfig

object PreprocFIPSYamlProtocol extends SharedYamlProtocol {
  implicit val preprocFIPSYamlFormat = yamlFormat3(FIPSConfig)
}

object PreprocFIPS extends StepConfigConfig {

  type ConfigType = FIPSConfig

  val yamlFormat = PreprocFIPSYamlProtocol.preprocFIPSYamlFormat

  val configType = classOf[FIPSConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: FIPSConfig) = {
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












