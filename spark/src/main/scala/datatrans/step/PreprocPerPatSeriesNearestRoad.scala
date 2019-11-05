package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import scopt._
import net.jcazevedo.moultingyaml._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocPerPatSeriesNearestRoadConfig(
  patgeo_data : String,
  nearestroad_data : String,
  maximum_search_radius : Double, // = 500,
  output_file : String
) extends StepConfig

object PerPatSeriesNearestRoadYamlProtocol extends DefaultYamlProtocol {
  implicit val perPatSeriesNearestRoadYamlFormat = yamlFormat4(PreprocPerPatSeriesNearestRoadConfig)
}

object PreprocPerPatSeriesNearestRoad extends StepConfigConfig {

  type ConfigType = PreprocPerPatSeriesNearestRoadConfig

  val yamlFormat = PerPatSeriesNearestRoadYamlProtocol.perPatSeriesNearestRoadYamlFormat

  val configType = classOf[PreprocPerPatSeriesNearestRoadConfig].getName()

  def main(args: Array[String]) {
    val parser = new OptionParser[PreprocPerPatSeriesNearestRoadConfig]("series_to_vector") {
      head("series_to_vector")
      opt[String]("patgeo_data").required.action((x,c) => c.copy(patgeo_data = x))
      opt[String]("nearestroad_data").required.action((x,c) => c.copy(nearestroad_data = x))
      opt[Double]("maximum_search_radius").action((x,c) => c.copy(maximum_search_radius = x))
      opt[String]("output_file").required.action((x,c) => c.copy(output_file = x))
    }

    val spark = SparkSession.builder().appName("datatrans preproc").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import PerPatSeriesNearestRoadYamlProtocol._

    parseInput[PreprocPerPatSeriesNearestRoadConfig](args) match {
      case Some(config) =>
        step(spark, config)

      case None =>
    }
    spark.stop()
  }

  def step(spark: SparkSession, config: PreprocPerPatSeriesNearestRoadConfig): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val hc = spark.sparkContext.hadoopConfiguration
      val output_file_path = new Path(config.output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        println(config.output_file + " exists")
      } else {
        val pddf0 = spark.read.format("csv").option("header", value = true).load(config.patgeo_data)
        val rows = pddf0.mapPartitions(partition => {
          val nearestRoad = new NearestRoad(config.nearestroad_data, config.maximum_search_radius)
          partition.map(r => {
            (r.getString(0), nearestRoad.getMinimumDistance(r.getString(1).toDouble, r.getString(2).toDouble))
          })
        })

        val df = rows.toDF("patient_num", "MajorRoadwayHighwayExposure")

        writeDataframe(hc, config.output_file, df)
      }

    }

  }

}
