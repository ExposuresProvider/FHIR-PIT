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

class EnvDataSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "EnvData" should "handle all columns" in {
    val tempDir = Files.createTempDirectory("env")

    val config = EnvDataSourceConfig(
      patgeo_data = "src/test/data/fhir_processed/2010/geo.csv",
      environmental_data = "src/test/data/other/env",
      output_file = s"${tempDir.toString()}/%i",
      start_date = stringToDateTime("2010-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      fips_data = "src/test/data/other/spatial/env/env.shp",
      indices = Seq(),
      statistics = Seq(),
      indices2 = Seq(
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
      ),
      offset_hours = 0
    )

    PreprocPerPatSeriesEnvData.step(spark, config)

    val toDouble = (x : String) => x.toDouble
    compareFileTree("src/test/data/other_processed/env/2010", tempDir.toString(), true, Map(
      "ozone_daily_8hour_maximum" -> toDouble,
      "pm25_daily_average" -> toDouble,
      "CO_ppbv" -> toDouble,
      "NO_ppbv" -> toDouble,
      "NO2_ppbv" -> toDouble,
      "NOX_ppbv" -> toDouble,
      "SO2_ppbv" -> toDouble,
      "ALD2_ppbv" -> toDouble,
      "FORM_ppbv" -> toDouble,
      "BENZ_ppbv" -> toDouble
    ))

    deleteRecursively(tempDir)

  }


}
