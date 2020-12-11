package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, Row, Encoder}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scopt._
import io.circe.{Decoder}
import io.circe.generic.semiauto._
import datatrans.Config._
import datatrans.Implicits._
import datatrans._

case class PreprocPerPatSeriesNearestPointConfig(
  patgeo_data : String,
  nearestpoint_data : String,
  output_file : String,
  feature_map : String,
  feature_name: String
)

object PreprocPerPatSeriesNearestPoint extends StepImpl {

  type ConfigType = PreprocPerPatSeriesNearestPointConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: PreprocPerPatSeriesNearestPointConfig): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val hc = spark.sparkContext.hadoopConfiguration
      val output_file_path = new Path(config.output_file)
      val output_file_file_system = output_file_path.getFileSystem(hc)

      if(output_file_file_system.exists(output_file_path)) {
        println(config.output_file + " exists")
      } else {
        val mapper = new Mapper(hc, config.feature_map)
        val nearest_point_mapping = mapper.nearest_point_map_map(config.feature_name)
        val distance_feature_name = nearest_point_mapping.distance_feature_name
        val attributes_to_features_map = nearest_point_mapping.attributes_to_features_map
        val (attributes, features) = attributes_to_features_map.unzip

        val schema = StructType(
          StructField("patient_num", StringType) +:
            StructField(distance_feature_name, DoubleType) +: features.toSeq.map(x => StructField(x, StringType)))

        val encoder : Encoder[Row] = RowEncoder(schema)

        val pddf0 = spark.read.format("csv").option("header", value = true).load(config.patgeo_data)
        val df = pddf0.mapPartitions(partition => {
          val nearest_point = new NearestPoint(config.nearestpoint_data)
          partition.map(r => {
            val distance_to_nearest_point = nearest_point.getDistanceToNearestPoint(r.getString(1).toDouble, r.getString(2).toDouble)
            Row.fromSeq(Seq(r.getString(0), distance_to_nearest_point) ++ attributes.map(attribute => nearest_point.getMatchedAttribute(attribute)))
          })
        })(encoder)

        writeDataframe(hc, config.output_file, df)
      }

    }

  }

}
