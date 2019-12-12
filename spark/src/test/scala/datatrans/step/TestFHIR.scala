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

  def deleteRecursively(file: Path): Unit = {
    if (Files.isDirectory(file)) {
      val stream = Files.newDirectoryStream(file)
      stream.iterator().asScala.foreach(deleteRecursively _)
      stream.close()
    } else {
      Files.delete(file)
    }
  }

  "FHIR" should "transform json" in {
    val tempDir = Files.createTempDirectory("fhir_processed")

    val config = PreprocFHIRConfig(
      input_directory = "src/test/data/fhir",
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

    getFileTree(new File("src/test/data/fhir_processed_expected")).foreach(f => {
      println("comparing " + f.getPath())
      val expected_path = f.getPath()
      val output_path = expected_path.replace("src/test/data/fhir_processed_expected", tempDir.toString())
      val op = Paths.get(output_path)
      assert(Files.isRegularFile(op))
      val f1 = readFile(output_path)
      val f2 = readFile(expected_path)
      val json1 = parse(f1).right.getOrElse(null)
      val json2 = parse(f2).right.getOrElse(null)
      if (json1 != null && json2 != null) {
        println("json1 = " + json1)
        println("json2 = " + json2)
        val patch = JsonDiff.diff(json1, json2, true)
        println("diff = " + patch)
        assert(json1 == json2)
      } else {
        println("f1 = " + f1)
        println("f2 = " + f2)
        val strdiff = f1 diff f2
        println("diff = " + strdiff)
        assert(f1 == f2)
      }

    })

    deleteRecursively(tempDir)

  }


}
