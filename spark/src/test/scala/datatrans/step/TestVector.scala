package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import datatrans.step.PreprocFHIRResourceType._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths, Path}
import org.scalatest.Assertions._
import java.nio.file.Files
import gnieh.diffson.circe._
import io.circe.parser._
import org.joda.time._
import org.joda.time.format._
import TestUtils._

class VectorSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "Vector" should "transform json to vector" in {
    val tempDir = Files.createTempDirectory("PatVec")

    val parser = ISODateTimeFormat.dateTimeParser()
    val config = PreprocPerPatSeriesToVectorConfig(
      input_directory = "src/test/data/fhir_processed/2010/Patient",
      output_directory = tempDir.toString,
      start_date = parser.parseDateTime("2010-01-01T00:00:00Z"),
      end_date = parser.parseDateTime("2011-01-01T00:00:00Z"),
      offset_hours = 0,
      med_map = "src/main/data/ICEES_Identifiers_v7 06.03.19_rxcui.json"
    )

    PreprocPerPatSeriesToVector.step(spark, config)

    compareFileTree("src/test/data/vector/2010/PatVec", tempDir.toString())

    deleteRecursively(tempDir)

  }


}
