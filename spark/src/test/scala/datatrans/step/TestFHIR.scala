package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import datatrans.step.PreprocFHIRResourceType._
import scala.collection.JavaConversions._
import java.io.File
import java.nio.file.{Files, Paths}
import org.scalatest.Assertions._

class FHIRSpec extends FlatSpec {
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  def getFileTree(f: File): Stream[File] =
    if (f.isDirectory)
      f.listFiles().toStream.flatMap(getFileTree)
    else
      Seq(f).toStream

  def readFile(f: String): String = {
    val source = scala.io.Source.fromFile(f)
    try source.mkString finally source.close()
  }

  "FHIR" should "transform json" in {
    val config = PreprocFHIRConfig(
      input_directory = "src/test/data/fhir",
      output_directory = "src/test/data/fhir_processed",
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

    getFileTree(new File("src/test/data/fhir_processed_expected")).foreach(f => {
      println("comparing " + f.getPath())
      val expected_path = f.getPath()
      val output_path = expected_path.replace("_expected","")
      val op = Paths.get(output_path)
      assert(Files.isRegularFile(op))
      val f1 = readFile(output_path)
      val f2 = readFile(expected_path)
      assert(f1 == f2)

    })

  }


}
