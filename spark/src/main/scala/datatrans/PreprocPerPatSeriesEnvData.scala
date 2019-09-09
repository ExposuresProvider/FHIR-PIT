package datatrans

import datatrans.Utils._
import org.apache.spark.sql.SparkSession
import org.joda.time._
import scopt._

import datatrans.environmentaldata._
import datatrans.Config._


object PreprocPerPatSeriesEnvData {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import MyYamlProtocol._

    parseInput[EnvDataSourceConfig](args) match {
      case Some(config) =>
        step(spark, config)
      case None =>
    }

    spark.stop()

  }

  def step(spark: SparkSession, config: EnvDataSourceConfig) = {
    time {
      val datasource = new EnvDataSource(spark, config)
      datasource.run()
    }
  }
}
