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


case class PerPatSeriesEnvDataFIPSConfig(
  patgeo_data : String,
  environmental_data : String,
  output_file : String
) extends StepConfig

object PreprocPerPatSeriesEnvDataFIPSYamlProtocol extends SharedYamlProtocol {
  implicit val preprocPerPatSeriesEnvDataFIPSYamlFormat = yamlFormat3(PerPatSeriesEnvDataFIPSConfig)
}

object PreprocPerPatSeriesEnvDataFIPS extends StepConfigConfig {

  type ConfigType = PerPatSeriesEnvDataFIPSConfig

  val yamlFormat = PreprocPerPatSeriesEnvDataFIPSYamlProtocol.preprocPerPatSeriesEnvDataFIPSYamlFormat

  val configType = classOf[PerPatSeriesEnvDataFIPSConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: PerPatSeriesEnvDataFIPSConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.patgeo_data
      log.info("loading patient_dimension from " + patient_dimension)
      val pddf0 = spark.read.format("csv").option("header", value = true).load(patient_dimension)
      val patl = pddf0.select("patient_num").map(r => r.getString(0)).collect.toList
      val n = patl.size

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file_all = config.output_file.replace("%i", "all")
      val output_file_all_path = new Path(output_file_all)
      val output_file_all_file_system = output_file_all_path.getFileSystem(hc)

      val df4 = spark.read.format("csv").option("header", value = true).load(output_file_all)

      withCounter(count =>

        patl.foreach((r) => {
          log.info("processing patient " + count.incrementAndGet() + " / " + n + " " + r)
          val output_file = config.output_file.replace("%i", r)

          writeDataframe(hc, output_file, df4.filter($"patient_num" === r).drop("patient_num"))
        })
      )

    }
  }

}












