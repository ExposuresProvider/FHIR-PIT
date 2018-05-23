package datatrans

import datatrans.Utils._
import datatrans.components.{DataSourceSelectorRunnerSparkJsValue, MkDataSourceSelectorFormatter}
import datatrans.environmentaldata.EnvDataSourceConfig
import org.apache.spark.sql.SparkSession
import org.joda.time._
import scopt._

import datatrans.environmentaldata._


object PreprocPerPatSeriesEnvData {

  def main(args: Array[String]) {
    val parser = new OptionParser[EnvDataSourceConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patient_dimension").action((x,c) => c.copy(patient_dimension = x))
      opt[String]("input_directory").required.action((x,c) => c.copy(input_directory = x))
      opt[String]("time_series").required.action((x,c) => c.copy(time_series = x))
      opt[String]("environmental_data").required.action((x,c) => c.copy(environmental_data = Some(x)))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
      opt[String]("start_date").required.action((x,c) => c.copy(start_date = DateTime.parse(x)))
      opt[String]("end_date").required.action((x,c) => c.copy(end_date = DateTime.parse(x)))
      opt[String]("output_format").action((x,c) => c.copy(output_format = x))
      opt[Unit]("coordinates").action((_,c) => c.copy(geo_coordinates = true))
      opt[Unit]("sequential").action((_,c) => c.copy(sequential = true))
      opt[Seq[Int]]("date_offsets").action((x,c) => c.copy(date_offsets = x))
      opt[Seq[String]]("query").action((x,c) => c.copy(indices = x))
      opt[Seq[String]]("statistics").action((x,c) => c.copy(statistics = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    parser.parse(args, EnvDataSourceConfig()) match {
      case Some(config) =>
        time {
          val pdif = config.patient_dimension

          DataSourceSelectorRunnerSparkJsValue.run(spark, pdif, "patient_num", config.sequential, config.time_series, MkDataSourceSelectorFormatter(new EnvSelector(), new EnvDataSource(config), new EnvFormatter(JSON)), config.output_file)
        }
      case None =>
    }

    spark.stop()

  }
}
