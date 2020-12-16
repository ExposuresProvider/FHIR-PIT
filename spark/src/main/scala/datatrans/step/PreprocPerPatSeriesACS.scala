package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils.{time, fileExists, writeDataframe}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import datatrans.Implicits._
import datatrans.{Mapper, GeoidFinder, StepImpl}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

case class PreprocPerPatSeriesACSConfig(
  time_series : String,
  acs_data : String,
  geoid_data : String,
  output_file : String,
  feature_map : String,
  feature_name: String
)

case class PatientCoordinates(
  patient_num: String,
  lat: Option[Double],
  lon : Option[Double]
)

object PreprocPerPatSeriesACS extends StepImpl {

  type ConfigType = PreprocPerPatSeriesACSConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config: PreprocPerPatSeriesACSConfig) : Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    time {

      val hc = spark.sparkContext.hadoopConfiguration

      if(fileExists(hc, config.output_file)) {
        println(config.output_file + " exists")
      } else {
        val mapper = new Mapper(hc, config.feature_map)
        val acs = mapper.geoid_map_map(config.feature_name)
        val geoid = acs.GEOID
        val schema = ScalaReflection.schemaFor[PatientCoordinates].dataType.asInstanceOf[StructType]
        val pddf0 = spark.read.format("csv").option("header", value = true).schema(schema).load(config.time_series).as[PatientCoordinates]

        val df = pddf0.mapPartitions(partition => {
          val geoidFinder = new GeoidFinder(config.geoid_data, "15000US")
          partition.flatMap(r =>
            r.lat match {
              case Some(lat) =>
                r.lon match {
                  case Some(lon) =>
                   val geoid = geoidFinder.getGeoidForLatLon(lat, lon)
                    Some((r.patient_num, geoid))
                  case _ =>
                    None
                }
              case _ =>
                None
            }
          )
        }).toDF("patient_num", geoid)

        val acs_df = spark.read.format("csv").option("header", value = true).load(config.acs_data)

        val acs_df_renamed = acs_df.select(
          (acs_df.col(geoid) +:
          acs.columns.toSeq.map {
            case (oldcolumnname, newcolumnname) => acs_df.col(oldcolumnname).as(newcolumnname)
          }) : _*
        )

        val table = df.join(acs_df_renamed, geoid).drop(geoid)

        writeDataframe(hc, config.output_file, table)
      }

    }
    
  }

}
