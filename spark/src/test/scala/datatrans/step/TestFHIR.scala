package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import datatrans.step.PreprocFHIRResourceType._

class FHIRSpec extends FlatSpec {
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

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
  }

}
