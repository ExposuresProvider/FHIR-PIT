package datatrans
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time.DateTime
import org.apache.log4j.{Logger, Level}
import squants.mass.{Kilograms, Grams, Pounds}
import squants.space.{Centimeters, Inches}
import scala.collection.mutable.ListBuffer

object Mapper {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  case class FHIRFeatureMapping(Condition: Option[Seq[Coding]], Observation: Option[Seq[Coding]], MedicationRequest: Option[Seq[Coding]], Procedure: Option[Seq[Coding]])

  case class GEOIDMapping(GEOID: String, columns: Map[String, String])

  case class FeatureMapping(FHIR: Option[Map[String, FHIRFeatureMapping]], GEOID: Option[Map[String, GEOIDMapping]])

  type CodingToFeatureMap = Map[Coding, String]

  def loadFeatureMap(hc : Configuration, feature_map_input_path : String) : (CodingToFeatureMap, CodingToFeatureMap, CodingToFeatureMap, CodingToFeatureMap, Map[String, GEOIDMapping]) = {
    val med_map_path = new Path(feature_map_input_path)
    val input_directory_file_system = med_map_path.getFileSystem(hc)

    val json = Utils.parseInputStreamYaml(input_directory_file_system.open(med_map_path))
    json.as[FeatureMapping] match {
      case Left(error) => throw new RuntimeException(error)
      case Right(obj) =>
        val cond_map = scala.collection.mutable.Map[Coding, String]()
        val med_map = scala.collection.mutable.Map[Coding, String]()
        val obs_map = scala.collection.mutable.Map[Coding, String]()
        val proc_map = scala.collection.mutable.Map[Coding, String]()
        obj.FHIR.foreach(obj =>
          for((feature_name, feature_mapping) <- obj) {
            for(
              condlist <- feature_mapping.Condition.toSeq;
              coding <- condlist
            ) {
              cond_map(coding) = feature_name
            }
            for(
              medlist <- feature_mapping.MedicationRequest.toSeq;
              coding <- medlist
            ) {
              med_map(coding) = feature_name
            }
            for(
              obslist <- feature_mapping.Observation.toSeq;
              coding <- obslist
            ) {
              obs_map(coding) = feature_name
            }
            for(
              proclist <- feature_mapping.Procedure.toSeq;
              coding <- proclist
            ) {
              proc_map(coding) = feature_name
            }
          }
        )
        (cond_map.toMap, obs_map.toMap, med_map.toMap, proc_map.toMap, obj.GEOID.getOrElse(Map()))
    }
  }

  def sort_by_effectiveDateTime(lab : Seq[Lab]) : Seq[Lab] =
    lab.sortWith((a, b) => {
      val at = DateTime.parse(a.effectiveDateTime, ISODateTimeFormat.dateTimeParser())
      val bt = DateTime.parse(b.effectiveDateTime, ISODateTimeFormat.dateTimeParser())
      if(at == bt && a.value != b.value) {
        log.info("warning: two labs in one encounter has same effectiveDateTime but different values")
      }
      at.isBefore(bt)
    })



  def map_lab(lab : Seq[Lab]) : Seq[(String, Any)] = {
    def filter_by_code(code : String) =
      sort_by_effectiveDateTime(lab.filter(lab => lab.coding.exists((x) => x.code == code)))
    val wbc = filter_by_code("6690-2") // 26464-8
    val hct = filter_by_code("20570-8") // 24360-0
    val plt = filter_by_code("26515-7") // 7773
    val fev1 = filter_by_code("20150-9") // 52485-0
    val fvc = filter_by_code("19870-5") // 52485-0
    val fev1fvc = filter_by_code("19926-5") // 52485-0
    val listBuf = new ListBuffer[(String, Any)]()

    def extractValue(lab: Lab) = lab.value.map(x => x.asInstanceOf[ValueQuantity].valueNumber).getOrElse(null)
    def extractFlag(lab: Lab) = lab.flag.getOrElse(null)
    def extractColumns(lab: Seq[Lab], prefix: String) = {
      if(!lab.isEmpty) {
        Seq(
          (f"${prefix}_FirstValue", extractValue(lab.head)),
          (f"${prefix}_FirstFlag", extractFlag(lab.head)),
          (f"${prefix}_LastValue", extractValue(lab.last)),
          (f"${prefix}_LastFlag", extractFlag(lab.last))
        )
      } else {
        Seq()
      }
    }

    def extractColumns2(lab: Seq[Lab], prefix: String) = {
      if(!lab.isEmpty) {
        Seq(
          (f"${prefix}_FirstValue", extractValue(lab.head)),
          (f"${prefix}_LastValue", extractValue(lab.last))
        )
      } else {
        Seq()
      }
    }

    extractColumns(wbc, "WBC") ++
    extractColumns(hct, "HCT") ++
    extractColumns(plt, "PLT") ++
    extractColumns(fev1fvc, "FEV1FVC") ++
    extractColumns2(fev1, "FEV1") ++
    extractColumns2(fvc, "FVC")

  }

  def match_str(pattern: String, str: String): Boolean =
    if (pattern.contains("*")) {
      pattern.r.pattern.matcher(str).matches()
    } else {
      pattern == str
    }

  def map_coding_to_feature(medmap : CodingToFeatureMap, coding : Coding) : Option[String] = 
    medmap.view.filter {
      case (coding_pattern, feature_name) =>
        match_str(coding_pattern.code, coding.code) && match_str(coding_pattern.system, coding.system)
    }.headOption.map (_._2)

  def map_race(race : Seq[String]) : String =
    if(race.isEmpty) {
      "Unknown"
    } else {
      race.head.trim match {
        case "2106-3" => "Caucasian"
        case "2054-5" => "African American"
        case "2028-9" => "Asian"
        case "2076-8" => "Native Hawaiian/Pacific Islander"
        case "1002-5" => "American/Alaskan Native"
        case _ => "Other(" + race.head + ")"
      }
    }

  def map_ethnicity(ethnicity : Seq[String]) : String =
    if(ethnicity.isEmpty) {
      "Unknown"
    } else {
      ethnicity.head match {
        case "2135-2" => "Hispanic"
        case "2186-5" => "Not Hispanic"
        case _ => "Unknown"
      }
    }

  def map_sex(sex : String) : String =
    sex match {
      case "male" => "Male"
      case "female" => "Female"
      case _ => "Unknown"
    }

  def map_bmi(bmi : Seq[Lab]) : Option[Double] = {
    def filter_by_code(code : String) =
      bmi.filter(bmi => bmi.coding.exists((x) => x.code == code))
    val bmiQuas = filter_by_code(LOINC.BMI)
    val heightQuas = filter_by_code(LOINC.BODY_HEIGHT)
    val weightQuas = filter_by_code(LOINC.BODY_WEIGHT)
    bmiQuas match {
      case Seq() =>
        (heightQuas, weightQuas) match {
          case (heightQ :: _ , weightQ :: _) =>
            val heightQua = heightQ.value.asInstanceOf[ValueQuantity]
            val weightQua = weightQ.value.asInstanceOf[ValueQuantity]
            val heightVal = heightQua.valueNumber
            val heightUnit = heightQua.unit
            val weightVal = weightQua.valueNumber
            val weightUnit = weightQua.unit
            val height = (heightUnit match {
              case Some("in") =>
                Inches
              case Some("[in_i]") =>
                Inches
              case Some("cm") =>
                Centimeters
              case _ =>
                throw new RuntimeException("unsupported unit " + heightUnit)
            })(heightVal) to Inches
            val weight = (weightUnit match {
              case Some("lbs") =>
                Pounds
              case Some("[lb_av]") =>
                Pounds
              case Some("kg") =>
                Kilograms
              case Some("g") =>
                Grams
              case _ =>
                throw new RuntimeException("unsupported unit " + weightUnit)
            })(weightVal) to Pounds
            Some(weight / math.pow(height, 2) * 703)
          case _ =>
            None
        }
      case bmiQua :: _ =>
        Some(bmiQua.value.asInstanceOf[ValueQuantity].valueNumber)
    }
  }

  val nearestRoad = Seq(
    "MajorRoadwayHighwayExposure"
  )

  val nearestRoad2 = Seq(
    "RoadwayDistanceExposure",
    "RoadwayType",
    "RoadwayAADT",
    "RoadwaySpeedLimit",
    "RoadwayLanes"
  )

  val demograph = Seq(
    "birth_date",
    "Sex",
    "Race",
    "Ethnicity")

  object LOINC {
    val BMI = "39156-5"
    val BODY_HEIGHT = "8302-2"
    val BODY_WEIGHT = "29463-7"
  }

  val envInputColumns1 = Seq("pm25", "o3")

  val envInputColumns2 = Seq(
    "pm25_daily_average",
    "ozone_daily_8hour_maximum",
    "CO_ppbv",
    "NO_ppbv",
    "NO2_ppbv", 
    "NOX_ppbv", 
    "SO2_ppbv", 
    "ALD2_ppbv",
    "FORM_ppbv",
    "BENZ_ppbv"
  )

  val mappedEnvStats = Seq("avg", "max")

  val studyPeriodMappedEnvStats = Seq("avg", "max")

  val mappedRawEnvColumns1 = (for(
    ftr <- envInputColumns1;
    stat <- mappedEnvStats
  ) yield f"${ftr}_${stat}")

  val mappedRawEnvColumns2 = envInputColumns2

  def getEnvOutputColumns(mappedRawEnvColumns : Seq[String]) : Seq[String] = for (
    mappedRawEnvColumn <- mappedRawEnvColumns;
    stat <- Seq("", "_prev_date", "_avg", "_max")
  ) yield f"${mappedRawEnvColumn}${stat}"

  val mappedEnvOutputColumns1 = getEnvOutputColumns(mappedRawEnvColumns1)

  val mappedEnvOutputColumns2 = getEnvOutputColumns(mappedRawEnvColumns2)

  val mappedEnvOutputColumns = mappedEnvOutputColumns1 ++ mappedEnvOutputColumns2

  val envFeatures1 = Seq("PM2.5", "Ozone")

  val envFeatures2 = Seq(
    ("Avg", "PM2.5"),
    ("Max", "Ozone"),
    ("Avg", "CO"),
    ("Avg", "NO"),
    ("Avg", "NO2"),
    ("Avg", "NOx"),
    ("Avg", "SO2"),
    ("Avg", "Acetaldehyde"),
    ("Avg", "Formaldehyde"),
    ("Avg", "Benzene")
  )


  val visitEnvFeatureMapping = (for(
    (ftr, icees_ftr) <- envInputColumns1 zip envFeatures1;
    (stat, icees_stat) <- mappedEnvStats zip Seq("Avg", "Max")
  ) yield (f"${ftr}_${stat}_prev_date", f"${icees_stat}24h${icees_ftr}Exposure")) ++ (for(
    (ftr, (icees_stat, icees_ftr)) <- envInputColumns2 zip envFeatures2
  ) yield (f"${ftr}_prev_date", f"${icees_stat}24h${icees_ftr}Exposure_2"))

  val patientEnvFeatureMapping = (for(
    (ftr, icees_ftr) <- envInputColumns1 zip envFeatures1;
    (stat, icees_stat) <- mappedEnvStats zip Seq("Avg", "Max");
    (stat_b, study_period_icees_stat) <- Seq("", "_avg", "_max") zip Seq("", "_StudyAvg", "_StudyMax")
  ) yield (f"${ftr}_${stat}${stat_b}", f"${icees_stat}Daily${icees_ftr}Exposure${study_period_icees_stat}")) ++ (for(
    (ftr, (icees_stat, icees_ftr)) <- envInputColumns2 zip envFeatures2
  ) yield (f"${ftr}_avg", f"${icees_stat}Daily${icees_ftr}Exposure_2"))

}

class Mapper(hc : Configuration, feature_map_input_path : String) {

  import Mapper.{CodingToFeatureMap, GEOIDMapping, loadFeatureMap}

  val (cond_map, obs_map, med_map, proc_map, geoid_map_map) : (CodingToFeatureMap, CodingToFeatureMap, CodingToFeatureMap, CodingToFeatureMap, Map[String, GEOIDMapping]) = loadFeatureMap(hc, feature_map_input_path)

  val meds = med_map.values.toSet
  val conds = cond_map.values.toSet
  val labs = obs_map.values.toSet
  val procs = obs_map.values.toSet

}
