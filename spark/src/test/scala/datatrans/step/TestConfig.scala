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
import sys.process._
import org.joda.time.DateTime
import TestUtils._
import datatrans.Utils._
import datatrans.{StepImpl, Step}
import datatrans.Config.parseYAML
import datatrans.StepImplicits._
import datatrans.SharedImplicits._

class ConfigSpec extends FlatSpec {
  
  "decoder" should "decode date time" in {
    assert(parseYAML[DateTime]("2010-01-01T00:00:00Z") != None)
  }

  "parseYAML" should "parse FHIR step implementation" in {
    assert(parseYAML[StepImpl]("""
function: datatrans.step.PreprocFHIR
arguments:
  input_directory: FHIR_merged
  output_directory: FHIR_processed
  resc_types: {}
  skip_preproc: []
""".stripMargin) != None)
  }

  "parseYAML" should "parse FHIR step config" in {
    assert(parseYAML[Any]("""
function: datatrans.step.PreprocFHIR
arguments:
  input_directory: FHIR_merged
  output_directory: FHIR_processed
  resc_types: {}
  skip_preproc: []
""".stripMargin) != None)
  }

  "parseYAML" should "parse FHIR step" in {
    assert(parseYAML[Step]("""
name: FHIR
dependsOn: []
skip: true
step: 
  function: datatrans.step.PreprocFHIR
  arguments:
    input_directory: FHIR_merged
    output_directory: FHIR_processed
    resc_types: {}
    skip_preproc: []
""".stripMargin) != None)
  }

}
