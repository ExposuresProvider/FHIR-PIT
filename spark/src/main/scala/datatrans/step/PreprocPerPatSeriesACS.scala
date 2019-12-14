package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import scopt._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import net.jcazevedo.moultingyaml._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocPerPatSeriesACSConfig(
                   time_series : String = "",
                   acs_data : String = "",
                   geoid_data : String = "",
                   output_file : String = ""
) extends StepConfig

object PerPatSeriesACSYamlProtocol extends DefaultYamlProtocol {
  implicit val perPatSeriesACSYamlFormat = yamlFormat4(PreprocPerPatSeriesACSConfig)
}

object PreprocPerPatSeriesACS extends StepConfigConfig {

  type ConfigType = PreprocPerPatSeriesACSConfig

  val yamlFormat = PerPatSeriesACSYamlProtocol.perPatSeriesACSYamlFormat

  val configType = classOf[PreprocPerPatSeriesACSConfig].getName()

  def step(spark: SparkSession, config: PreprocPerPatSeriesACSConfig) : Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val hc = spark.sparkContext.hadoopConfiguration

      if(fileExists(hc, config.output_file)) {
        println(config.output_file + " exists")
      } else {
        val pddf0 = spark.read.format("csv").option("header", value = true).load(config.time_series)

        val df = pddf0.mapPartitions(partition => {
          val geoidFinder = new GeoidFinder(config.geoid_data, "15000US")
          partition.map(r => {
            val geoid = geoidFinder.getGeoidForLatLon(r.getString(1).toDouble, r.getString(2).toDouble)
            (r.getString(0), geoid)
          })
        }).toDF("patient_num", "GEOID")

        val acs_df = spark.read.format("csv").option("header", value = true).load(config.acs_data)

        val table = df.join(acs_df, "GEOID")
          .withColumnRenamed("EstPropPersons5PlusNoEnglish", "EstProbabilityESL")
          .withColumnRenamed("median_HH_inc", "EstHouseholdIncome")
          .withColumnRenamed("nHwtindiv", "EstProbabilityNonHispWhite")
          .withColumnRenamed("prp_HSminus", "EstProbabilityHighSchoolMaxEducation")
          .withColumnRenamed("prp_nHwHHs", "EstProbabilityHouseholdNonHispWhite")
          .withColumnRenamed("prp_no_auto", "EstProbabilityNoAuto")
          .withColumnRenamed("prp_not_insured", "EstProbabilityNoHealthIns")
          .withColumnRenamed("total_pop2016", "EstResidentialDensity")
          .withColumnRenamed("total_25plus", "EstResidentialDensity25Plus")

        writeDataframe(hc, config.output_file, table)
      }

    }
    
  }

}
