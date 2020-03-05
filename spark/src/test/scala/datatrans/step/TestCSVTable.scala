package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths, Path}
import org.scalatest.Assertions._
import java.nio.file.Files
import gnieh.diffson.circe._
import io.circe.parser._
import TestUtils._
import datatrans.Utils._

class CSVTableSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "EnvData" should "handle subset columns" in {
    val tempDir = Files.createTempDirectory("icees")
    val tempDir2 = Files.createTempDirectory("icees2")

    val config = PreprocPerPatSeriesCSVTableConfig(
      patient_file = "src/test/data/vector/2010/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2/aggregate"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv"
      ),
      output_dir = f"${tempDir.toString()}",
      start_date = stringToDateTime("2010-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      offset_hours = 0
    )

    PreprocPerPatSeriesCSVTable.step(spark, config)

    val config2 = PreprocCSVTableConfig(
      input_dir = f"${tempDir.toString()}/2010",
      output_dir = f"${tempDir2.toString()}/2010",
      start_date = stringToDateTime("2010-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      deidentify = Seq[String](),
      offset_hours = 0
    )

    PreprocCSVTable.step(spark, config2)

    val toDouble = (x : String) => x.toDouble
    val typemap = Map(
      "ozone_daily_8hour_maximum" -> toDouble,
      "pm25_daily_average" -> toDouble,
      "ozone_daily_8hour_maximum_max" -> toDouble,
      "pm25_daily_average_max" -> toDouble,
      "ozone_daily_8hour_maximum_avg" -> toDouble,
      "pm25_daily_average_avg" -> toDouble,
      "ozone_daily_8hour_maximum_min" -> toDouble,
      "pm25_daily_average_min" -> toDouble,
      "ozone_daily_8hour_maximum_stddev" -> toDouble,
      "pm25_daily_average_stddev" -> toDouble,
      "Max24hOzoneExposure_2" -> toDouble,
      "NO2_ppbv_avg" -> toDouble
    )
    compareFileTree("src/test/data/icees", tempDir.toString(), true, typemap)

    compareFileTree("src/test/data/icees2", tempDir2.toString(), true, typemap)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)

  }


}
