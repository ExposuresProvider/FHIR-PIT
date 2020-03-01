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


case class AddYearConfig(
  input_file : String,
  output_file : String,
  date_index: String,
  year_index : String
) extends StepConfig

object AddYearYamlProtocol extends SharedYamlProtocol {
  implicit val preprocAddYearYamlFormat = yamlFormat4(AddYearConfig)
}

/**
  *  split preagg into individual files for patients 
  */
object PreprocAddYear extends StepConfigConfig {

  type ConfigType = AddYearConfig

  val yamlFormat = AddYearYamlProtocol.preprocAddYearYamlFormat

  val configType = classOf[AddYearConfig].getName()

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: AddYearConfig) = {
    time {
      import spark.implicits._

      val patient_dimension = config.input_file
      log.info("loading input dataframe from " + patient_dimension)
      val df = spark.read.format("csv").option("header", value = true).load(patient_dimension)

      val hc = spark.sparkContext.hadoopConfiguration

      val output_file = config.output_file

      val df_with_year = df.withColumn(config.year_index, year(df.col(config.date_index)))

      writeDataframe(hc, output_file, df_with_year)
    }
  }

}
