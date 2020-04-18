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
import diffson.circe._
import io.circe.parser._
import TestUtils._

class FHIRSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  "FHIR" should "transform json" in {
    val tempDir = Files.createTempDirectory("fhir_processed")

    val config = PreprocFHIRConfig(
      input_directory = "src/test/data/fhir/2010",
      output_directory = tempDir.toString,
      resc_types = Map(
        EncounterResourceType -> "Encounter",
        PatientResourceType -> "Patient",
        LabResourceType -> "Observation_Labs",
        ConditionResourceType -> "Condition",
        MedicationRequestResourceType -> "MedicationRequest",
        ProcedureResourceType -> "Procedure"
      ),
      skip_preproc = Seq()
    )

    PreprocFHIR.step(spark, config)

    compareFileTree("src/test/data/fhir_processed/2010", tempDir.toString())

    deleteRecursively(tempDir)

  }


}
