package datatrans.step

import java.util.concurrent.atomic.AtomicInteger

import datatrans.Utils.{time, fileExists, writeDataframe}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, Row}
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
        val pddf0 = spark.read.format("csv").option("header", value = true).load(config.time_series)

        val df = pddf0.mapPartitions(partition => {
          val geoidFinder = new GeoidFinder(config.geoid_data, "15000US")
          partition.flatMap(r => {
            val patient_num = r.getString(r.fieldIndex("patient_num"))
            val latstr = r.getString(r.fieldIndex("lat"))
            val lonstr = r.getString(r.fieldIndex("lon"))
            if (latstr == "" || lonstr == "")
              None
            else {
              (try {
                Some ( latstr.toDouble,
                  lonstr.toDouble )
              } catch {
                case e : Exception =>
                  println(s"cannot convert [$latstr, $lonstr]")
                  None
              }).flatMap {
                case (lat, lon) => geoidFinder.getGeoidForLatLon(lat, lon).map(geoid => (patient_num, geoid))
              }
            }
          })
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
