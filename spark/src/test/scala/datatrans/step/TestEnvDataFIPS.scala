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

class EnvDataFIPSSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  val toDouble = (x : String) => if (x == "") null else x.toDouble

  val m = Map(
    "ozone_daily_8hour_maximum" -> toDouble,
    "pm25_daily_average" -> toDouble,
    "CO_ppbv" -> toDouble,
    "NO_ppbv" -> toDouble,
    "NO2_ppbv" -> toDouble,
    "NOX_ppbv" -> toDouble,
    "SO2_ppbv" -> toDouble,
    "ALD2_ppbv" -> toDouble,
    "FORM_ppbv" -> toDouble,
    "BENZ_ppbv" -> toDouble,
    "ozone_daily_8hour_maximum_max" -> toDouble,
    "pm25_daily_average_max" -> toDouble,
    "CO_ppbv_max" -> toDouble,
    "NO_ppbv_max" -> toDouble,
    "NO2_ppbv_max" -> toDouble,
    "NOX_ppbv_max" -> toDouble,
    "SO2_ppbv_max" -> toDouble,
    "ALD2_ppbv_max" -> toDouble,
    "FORM_ppbv_max" -> toDouble,
    "BENZ_ppbv_max" -> toDouble,
    "ozone_daily_8hour_maximum_min" -> toDouble,
    "pm25_daily_average_min" -> toDouble,
    "CO_ppbv_min" -> toDouble,
    "NO_ppbv_min" -> toDouble,
    "NO2_ppbv_min" -> toDouble,
    "NOX_ppbv_min" -> toDouble,
    "SO2_ppbv_min" -> toDouble,
    "ALD2_ppbv_min" -> toDouble,
    "FORM_ppbv_min" -> toDouble,
    "BENZ_ppbv_min" -> toDouble,
    "ozone_daily_8hour_maximum_avg" -> toDouble,
    "pm25_daily_average_avg" -> toDouble,
    "CO_ppbv_avg" -> toDouble,
    "NO_ppbv_avg" -> toDouble,
    "NO2_ppbv_avg" -> toDouble,
    "NOX_ppbv_avg" -> toDouble,
    "SO2_ppbv_avg" -> toDouble,
    "ALD2_ppbv_avg" -> toDouble,
    "FORM_ppbv_avg" -> toDouble,
    "BENZ_ppbv_avg" -> toDouble,
    "ozone_daily_8hour_maximum_stddev" -> toDouble,
    "pm25_daily_average_stddev" -> toDouble,
    "CO_ppbv_stddev" -> toDouble,
    "NO_ppbv_stddev" -> toDouble,
    "NO2_ppbv_stddev" -> toDouble,
    "NOX_ppbv_stddev" -> toDouble,
    "SO2_ppbv_stddev" -> toDouble,
    "ALD2_ppbv_stddev" -> toDouble,
    "FORM_ppbv_stddev" -> toDouble,
    "BENZ_ppbv_stddev" -> toDouble,
    "ozone_daily_8hour_maximum_prev_date" -> toDouble,
    "pm25_daily_average_prev_date" -> toDouble,
    "CO_ppbv_prev_date" -> toDouble,
    "NO_ppbv_prev_date" -> toDouble,
    "NO2_ppbv_prev_date" -> toDouble,
    "NOX_ppbv_prev_date" -> toDouble,
    "SO2_ppbv_prev_date" -> toDouble,
    "ALD2_ppbv_prev_date" -> toDouble,
    "FORM_ppbv_prev_date" -> toDouble,
    "BENZ_ppbv_prev_date" -> toDouble,
    "lat" -> toDouble,
    "lon" -> toDouble
  )

  val indices = Seq(
        "ozone_daily_8hour_maximum",
        "pm25_daily_average",
	"CO_ppbv",
	"NO_ppbv",
	"NO2_ppbv",
	"NOX_ppbv",
	"SO2_ppbv",
	"ALD2_ppbv",
	"FORM_ppbv",
	"BENZ_ppbv"
  )

  val statistics = Seq("max", "min", "avg", "stddev")

  def test(inp: String, outp: String) : Unit = {
    val tempDir = Files.createTempDirectory("env")
    val tempDir2 = Files.createTempDirectory("env2")
    val tempDir3 = Files.createTempDirectory("env3")

    val config0 = FIPSConfig(
      patgeo_data = "src/test/data/fhir_processed/2010/geo.csv",
      output_file = s"${tempDir.toString()}/geoids.csv",
      fips_data = "src/test/data/other/spatial/env/env.shp"
    )

    PreprocFIPS.step(spark, config0)

    val config = EnvDataFIPSConfig(
      environmental_data = f"src/test/data/other/$inp",
      output_file = s"${tempDir.toString()}/preagg",
      start_date = stringToDateTime("2009-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      fips_data = s"${tempDir.toString()}/geoids.csv",
      indices = indices,
      offset_hours = 0
    )

    PreprocEnvDataFIPS.step(spark, config)

    val config3 = SplitConfig(
      input_file = s"${tempDir.toString()}/preagg",
      split_index = "patient_num",
      output_dir = s"${tempDir2.toString()}"
    )

    PreprocSplit.step(spark, config3)

    val config2 = EnvDataAggregateConfig(
      input_dir=s"${tempDir2.toString()}",
      output_dir = s"${tempDir3.toString()}",
      statistics = statistics,
      indices = indices
    )

    PreprocEnvDataAggregate.step(spark, config2)


    compareFileTree(f"src/test/data/other_processed/$outp/fips", tempDir.toString(), true, m)
    compareFileTree(f"src/test/data/other_processed/$outp/split", tempDir2.toString(), true, m)
    compareFileTree(f"src/test/data/other_processed/$outp/aggregate", tempDir3.toString(), true, m)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)
    deleteRecursively(tempDir3)
  }

  "EnvDataFIPS" should "handle all columns" in {
    test("env", "env2")
  }

  "EnvDataFIPS" should "handle union columns inside of schema" in {
    test("envfips2", "envfips2")
  }

  "EnvDataFIPS" should "handle union columns outside of schema" in {
    test("envfips3", "envfips3")
  }

}
