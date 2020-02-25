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

    val config = PreprocCSVTableConfig(
      patient_file = "src/test/data/vector/2010/PatVec",
      environment_file = None,
      environment2_file = Some("src/test/data/other_processed/env2"),
      input_files = Seq[String](
        "src/test/data/other_processed/2010/acs.csv",
        "src/test/data/other_processed/2010/acs2.csv",
        "src/test/data/other_processed/2010/nearestroad.csv",
        "src/test/data/other_processed/2010/nearestroad2.csv"
      ),
      output_file = tempDir.toString(),
      start_date = stringToDateTime("2010-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      deidentify = Seq[String]()
    )

    PreprocCSVTable.step(spark, config)

    val toDouble = (x : String) => x.toDouble
    compareFileTree("src/test/data/icees/2010", tempDir.toString(), true, Map(
      "ozone_daily_8hour_maximum" -> toDouble,
      "pm25_daily_average" -> toDouble
    ))

    deleteRecursively(tempDir)

  }


}
