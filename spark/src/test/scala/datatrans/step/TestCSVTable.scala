package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths, Path}
import org.scalatest.Assertions._
import java.nio.file.Files
import diffson.circe._
import io.circe.parser._
import TestUtils._
import datatrans.Utils._

class CSVTableSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "CSVTable" should "handle subset columns" in {
    val tempDir = Files.createTempDirectory("icees")
    val tempDir2 = Files.createTempDirectory("icees2")

    val config = PreprocPerPatSeriesCSVTableConfig(
      patient_file = "src/test/data/vector/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2/aggregate"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv",
        "src/test/data/other_processed/2010/cafo.csv",
        "src/test/data/other_processed/2010/landfill.csv"
      ),
      output_dir = f"${tempDir.toString()}",
      study_periods = Seq("2010")
    )

    PreprocPerPatSeriesCSVTable.step(spark, config)

    val config2 = PreprocCSVTableConfig(
      input_dir = f"${tempDir.toString()}/2010",
      output_dir = f"${tempDir2.toString()}/2010",
      deidentify = Seq[String](),
      offset_hours = 0,
      feature_map = "config/icees_features.yaml"
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
    compareFileTree(tempDir.toString(), "src/test/data/icees", true, typemap)

    compareFileTree(tempDir2.toString(), "src/test/data/icees2", true, typemap)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)

  }

  "CSVTable" should "handle inactive years" in {
    val tempDir = Files.createTempDirectory("icees")
    val tempDir2 = Files.createTempDirectory("icees2")

    val config = PreprocPerPatSeriesCSVTableConfig(
      patient_file = "src/test/data/vector/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2/aggregate"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv",
        "src/test/data/other_processed/2010/cafo.csv",
        "src/test/data/other_processed/2010/landfill.csv"
      ),
      output_dir = f"${tempDir.toString()}",
      study_periods = Seq("2009", "2010")
    )

    PreprocPerPatSeriesCSVTable.step(spark, config)

    val config2 = PreprocCSVTableConfig(
      input_dir = f"${tempDir.toString()}/2009",
      output_dir = f"${tempDir2.toString()}/2009",
      deidentify = Seq[String](),
      offset_hours = 0,
      feature_map = "config/icees_features.yaml"
    )

    PreprocCSVTable.step(spark, config2)

    val toDouble = (x : String) => if (x == "") null else x.toDouble
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
    compareFileTree(tempDir.resolve("2009").toString(), "src/test/data/icees_inactive/2009", true, typemap)

    compareFileTree(tempDir2.resolve("2009").toString(), "src/test/data/icees2_inactive/2009", true, typemap)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)

  }

  "CSVTable" should "convert twice" in {
    val tempDir = Files.createTempDirectory("icees")
    val tempDir2 = Files.createTempDirectory("icees2")

    val config = PreprocPerPatSeriesCSVTableConfig(
      patient_file = "src/test/data/vector_twice/2010/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2/aggregate"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv",
        "src/test/data/other_processed/2010/cafo.csv",
        "src/test/data/other_processed/2010/landfill.csv"
      ),
      output_dir = f"${tempDir.toString()}",
      study_periods = Seq("2010")
    )

    PreprocPerPatSeriesCSVTable.step(spark, config)

    val config2 = PreprocCSVTableConfig(
      input_dir = f"${tempDir.toString()}/2010",
      output_dir = f"${tempDir2.toString()}/2010",
      deidentify = Seq[String](),
      offset_hours = 0,
      feature_map = "config/icees_features.yaml"
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
    compareFileTree(tempDir.toString(), "src/test/data/icees_twice", true, typemap)

    compareFileTree(tempDir2.toString(), "src/test/data/icees2_twice", true, typemap)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)

  }

  "CSVTable" should "convert thrice" in {
    val tempDir = Files.createTempDirectory("icees")
    val tempDir2 = Files.createTempDirectory("icees2")

    val config = PreprocPerPatSeriesCSVTableConfig(
      patient_file = "src/test/data/vector_thrice/2010/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2/aggregate"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv",
        "src/test/data/other_processed/2010/cafo.csv",
        "src/test/data/other_processed/2010/landfill.csv"
      ),
      output_dir = f"${tempDir.toString()}",
      study_periods = Seq("2010")
    )

    PreprocPerPatSeriesCSVTable.step(spark, config)

    val config2 = PreprocCSVTableConfig(
      input_dir = f"${tempDir.toString()}/2010",
      output_dir = f"${tempDir2.toString()}/2010",
      deidentify = Seq[String](),
      offset_hours = 0,
      feature_map = "config/icees_features.yaml"
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
    compareFileTree(tempDir.toString(), "src/test/data/icees_thrice", true, typemap)

    compareFileTree(tempDir2.toString(), "src/test/data/icees2_thrice", true, typemap)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)

  }

}
