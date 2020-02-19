let ResourceTypes : Type = {
    Condition: Text,
    Lab: Text,
    Encounter: Text,
    MedicationRequest: Text,
    Patient: Text,
    Procedure: Text
}

let YearConfig : Type = {
    year : Natural,
    skip : {
      toVector : Bool,
      envDataSource : Bool,
      envCSVTable : Bool
    }
}

let resc_types = {
    Condition = "Condition",
    Lab = "Lab",
    Encounter = "Encounter",
    MedicationRequest = "MedicationRequest",
    Patient = "Patient",
    Procedure = "Procedure"
}

let FhirConfig : Type = {
    skip: {
      fhir: Bool,
      acs : Bool,
      acs2 : Bool,
      nearestRoad : Bool,
      nearestRoad2 : Bool
    },
    skip_preproc: List Text    
}

in λ(report : Text) → λ(progress : Text) → λ(basedirinput : Text) → λ(basedir : Text) → λ(basediroutput : Text) → λ(fhirConfig : FhirConfig) → λ(skipList : List YearConfig) →

let GenericStep : Type → Type = λ(a : Type) → {
    name : Text,
    dependsOn: List Text,
    skip : Bool,
    step : {
        function : Text,
        arguments : a
    }
}

let FHIRStep : Type = GenericStep {
    input_directory: Text,
    output_directory: Text,
    resc_types: ResourceTypes,
    skip_preproc: List Text
}

let PerPatSeriesToVectorStep : Type = GenericStep {
    input_directory: Text,
    output_directory: Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer,
    med_map: Text
}

let EnvDataSourceStep : Type = GenericStep {
    patgeo_data: Text,
    environmental_data: Text,
    fips_data: Text,
    output_file: Text,
    indices: List Text,
    statistics: List Text,
    indices2: List Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer
}

let PerPatSeriesACSStep : Type = GenericStep {
    time_series : Text,
    acs_data : Text,
    geoid_data : Text,
    output_file : Text
}

let PerPatSeriesNearestRoadStep : Type = GenericStep {
    patgeo_data : Text,
    nearestroad_data : Text,
    maximum_search_radius : Double,
    output_file : Text
}

let EnvCSVTableStep : Type = GenericStep {
    patient_file : Text,
    environment_file : Text,
    input_files : List Text,
    output_file : Text,
    start_date : Text,
    end_date : Text,
    deidentify : List Text
}

let Step : Type = <
    FHIR : FHIRStep |
    ToVector : PerPatSeriesToVectorStep |
    EnvDataSource : EnvDataSourceStep |
    ACS : PerPatSeriesACSStep |
    NearestRoad : PerPatSeriesNearestRoadStep |
    EnvCSVTable : EnvCSVTableStep
>

let Config: Type = {
    report_output : Text,
    progress_output : Text,
    steps : List Step
}

let start_year = λ(year : Natural) → "${Natural/show year}-01-01T00:00:00-05:00"
let end_year = λ(year : Natural) → start_year (year + 1)
let patgeo = "${basedir}/FHIR_processed/geo.csv"
let acs = "${basedir}/other_processed/acs.csv"
let acs2 = "${basedir}/other_processed/acs2.csv"
let nearestroad = "${basedir}/other_processed/nearestroad.csv"
let nearestroad2 = "${basedir}/other_processed/nearestroad2.csv"

let fhirStep = λ(skip : Bool) → λ(skip_preproc : List Text) → Step.FHIR {
    name = "FHIR",
    dependsOn = [] : List Text,
    skip = skip,
    step = {
        function = "datatrans.step.PreprocFHIRConfig",
        arguments = {
            input_directory = "${basedirinput}/FHIR_merged",
            output_directory = "${basedir}/FHIR_processed",
            resc_types = resc_types,
            skip_preproc = skip_preproc
        }
    }
}

let toVectorStep = λ(skip : Bool) → λ(year : Natural) → Step.ToVector {
  name = "PerPatSeriesToVector${Natural/show year}",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesToVectorConfig",
    arguments = {
      input_directory = "${basedir}/FHIR_processed/Patient",
      output_directory = "${basedir}/FHIR_vector/${Natural/show year}/PatVec",
      start_date = start_year year,
      end_date = end_year year,
      offset_hours = -5,
      med_map = "${basedir}/other/medical/icees_features_rxnorm.json"
    }
  }
}

let envDataSourceStep = λ(skip : Bool) → λ(year : Natural) → Step.EnvDataSource {
  name = "EnvDataSource${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.EnvDataSourceConfig",
    arguments = {
      patgeo_data = patgeo,
      environmental_data = "${basedirinput}/other/env",
      fips_data = "${basedirinput}/other/spatial/env/US_Census_Tracts_LCC/US_Census_Tracts_LCC.shp",
      output_file = "${basedir}/other_processed/env/${Natural/show year}/%i",
      indices = [] : List Text,
      statistics = [] : List Text,
      indices2 = [
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
      ],
      start_date = start_year year,
      end_date = end_year year,
      offset_hours = -5
    }
  }
}

let acsStep = λ(skip : Bool) → Step.ACS {
  name = "PerPatSeriesACS",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesACSConfig",
    arguments = {
      time_series = patgeo,
      acs_data = "${basedirinput}/other/spatial/acs/ACS_NC_2016_with_column_headers.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acs
    }
  }
}

let acs2Step = λ(skip : Bool) → Step.ACS {
  name = "PerPatSeriesACS2",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesACS2Config",
    arguments = {
      time_series = patgeo,
      acs_data = "${basedirinput}/other/spatial/acs/Appold_trans_geo_cross_02.10.10 - trans_geo_cross.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acs2
    }
  }
}

let nearestRoadStep = λ(skip : Bool) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoad",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoadConfig",
    arguments = {
      patgeo_data = patgeo,
      nearestroad_data = "${basedirinput}/other/spatial/nearestroad/tl_2015_allstates_prisecroads_lcc.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestroad
    }
  }
}

let nearestRoad2Step = λ(skip : Bool) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoad2",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoad2Config",
    arguments = {
      patgeo_data = patgeo,
      nearestroad_data = "${basedirinput}/other/spatial/nearestroad2/hpms2016_major_roads_lcc.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestroad2
    }
  }
}

let envCSVTableStep = λ(skip : Bool) → λ(year : Natural) → Step.EnvCSVTable {
  name = "EnvCSVTable${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}",
    "PerPatSeriesACS",
    "PerPatSeriesACS2",
    "PerPatSeriesNearestRoad",
    "PerPatSeriesNearestRoad2",
    "EnvDataSource${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocCSVTableConfig",
    arguments = {
      patient_file = "${basedir}/FHIR_vector/${Natural/show year}/PatVec",
      environment_file = "${basedir}/other_processed/env",
      input_files = [
        acs,
        acs2,
        nearestroad,
	nearestroad2
      ],
      output_file = "${basediroutput}/icees/${Natural/show year}",
      start_date = start_year year,
      end_date = end_year year,
      deidentify = [] : List Text
    }
  }
}

in {
  report_output = report,
  progress_output = progress,
  steps =
    [
      fhirStep fhirConfig.skip.fhir fhirConfig.skip_preproc,
      acsStep fhirConfig.skip.acs,
      acs2Step fhirConfig.skip.acs2,
      nearestRoadStep fhirConfig.skip.nearestRoad,
      nearestRoad2Step fhirConfig.skip.nearestRoad2
    ] # List/fold YearConfig skipList (List Step) (λ(yearSkip : YearConfig) → λ(stepList : List Step) → [
      toVectorStep yearSkip.skip.toVector yearSkip.year,
      envDataSourceStep yearSkip.skip.envDataSource yearSkip.year,
      envCSVTableStep yearSkip.skip.envCSVTable yearSkip.year
    ] # stepList) ([] : List Step)
} : Config