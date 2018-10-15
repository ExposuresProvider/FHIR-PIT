package datatrans

import datatrans.Utils._
import datatrans.environmentaldata.EnvDataSourceConfig
import org.apache.spark.sql.SparkSession
import org.joda.time._
import scopt._

import datatrans.environmentaldata._


object PreprocPerPatSeriesEnvData {

  def main(args: Array[String]) {
    val parser = new OptionParser[EnvDataSourceConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patgeo_data").required.action((x,c) => c.copy(patgeo_data = x))
      opt[String]("fips_data").required.action((x,c) => c.copy(fips_data = x))
      opt[String]("environmental_data").required.action((x,c) => c.copy(environmental_data = x))
      opt[String]("start_date").required.action((x,c) => c.copy(start_date = DateTime.parse(x)))
      opt[String]("end_date").required.action((x,c) => c.copy(end_date = DateTime.parse(x)))
      opt[String]("output_file").action((x,c) => c.copy(output_file = x))
      opt[Seq[String]]("query").action((x,c) => c.copy(indices = x))
      opt[Seq[String]]("indices2").action((x,c) => c.copy(indices2 = x))
      opt[Seq[String]]("statistics").action((x,c) => c.copy(statistics = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    parser.parse(args, EnvDataSourceConfig()) match {
      case Some(config) =>
        time {
          val datasource = new EnvDataSource(config)
          datasource.run(spark)
        }
      case None =>
    }

    spark.stop()

  }
}
