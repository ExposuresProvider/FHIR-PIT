let ResourceTypes : Type = {
    Condition: Text,
    Lab: Text,
    Encounter: Text,
    MedicationRequest: Text,
    Patient: Text,
    Procedure: Text
}

let YearSkip : Type = {
    year : Natural,
    skip : {
      fhir : Bool,
      toVector : Bool,
      envDataSource : Bool,
      acs : Bool,
      acs2 : Bool,
      nearestRoad : Bool,
      nearestRoad2 : Bool,
      envCSVTable : Bool
    },
    resourceTypes: ResourceTypes,
    skip_preproc: List Text
}

in λ(report : Text) → λ(progress : Text) → λ(basedirinput : Text) → λ(basedir : Text) → λ(basediroutput : Text) → λ(skipList : List YearSkip) →

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
    end_date: Text
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
let patgeo = λ(year : Natural) → "${basedir}/FHIR_processed/${Natural/show year}/geo.csv"
let acs = λ(year : Natural) → "${basedir}/other_processed/${Natural/show year}/acs.csv"
let acs2 = λ(year : Natural) → "${basedir}/other_processed/${Natural/show year}/acs2.csv"
let nearestroad = λ(year : Natural) → "${basedir}/other_processed/${Natural/show year}/nearestroad.csv"
let nearestroad2 = λ(year : Natural) → "${basedir}/other_processed/${Natural/show year}/nearestroad2.csv"

let fhirStep = λ(skip : Bool) → λ(year : Natural) → λ(resc_types : ResourceTypes) → λ(skip_preproc : List Text) → Step.FHIR {
    name = "FHIR${Natural/show year}",
    dependsOn = [] : List Text,
    skip = skip,
    step = {
        function = "datatrans.step.PreprocFHIRConfig",
        arguments = {
            input_directory = "${basedirinput}/FHIR/${Natural/show year}",
            output_directory = "${basedir}/FHIR_processed/${Natural/show year}",
            resc_types = resc_types,
            skip_preproc = skip_preproc
        }
    }
}

let toVectorStep = λ(skip : Bool) → λ(year : Natural) → Step.ToVector {
  name = "PerPatSeriesToVector${Natural/show year}",
  dependsOn = [
    "FHIR${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesToVectorConfig",
    arguments = {
      input_directory = "${basedir}/FHIR_processed/${Natural/show year}/Patient",
      output_directory = "${basedir}/FHIR_processed/${Natural/show year}/PatVec",
      start_date = start_year year,
      end_date = end_year year,
      offset_hours = -5,
      med_map = "${basedir}/other/medical/ICEES_Identifiers_v7 06.03.19_rxcui.json"
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
      patgeo_data = patgeo year,
      environmental_data = "${basedirinput}/other/env",
      fips_data = "${basedirinput}/other/spatial/env/US_Census_Tracts_LCC/US_Census_Tracts_LCC.shp",
      output_file = "${basedir}/other_processed/${Natural/show year}/env/%i",
      indices = [] : List Text,
      statistics = [] : List Text,
      indices2 = [
        "ozone_daily_8hour_maximum",
        "pm25_daily_average"
      ],
      start_date = start_year year,
      end_date = end_year year
    }
  }
}

let acsStep = λ(skip : Bool) → λ(year : Natural) → Step.ACS {
  name = "PerPatSeriesACS${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesACSConfig",
    arguments = {
      time_series = patgeo year,
      acs_data = "${basedirinput}/other/spatial/acs/ACS_NC_2016_with_column_headers.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acs year
    }
  }
}

let acs2Step = λ(skip : Bool) → λ(year : Natural) → Step.ACS {
  name = "PerPatSeriesACS2${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesACS2Config",
    arguments = {
      time_series = patgeo year,
      acs_data = "${basedirinput}/other/spatial/acs/Appold_trans_geo_cross_02.10.10 - trans_geo_cross.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acs2 year
    }
  }
}

let nearestRoadStep = λ(skip : Bool) → λ(year : Natural) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoad${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoadConfig",
    arguments = {
      patgeo_data = patgeo year,
      nearestroad_data = "${basedirinput}/other/spatial/nearestroad/tl_2015_allstates_prisecroads_lcc.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestroad year
    }
  }
}

let nearestRoad2Step = λ(skip : Bool) → λ(year : Natural) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoad2${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoad2Config",
    arguments = {
      patgeo_data = patgeo year,
      nearestroad_data = "${basedirinput}/other/spatial/nearestroad2/hpms2016_major_roads.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestroad2 year
    }
  }
}

let envCSVTableStep = λ(skip : Bool) → λ(year : Natural) → Step.EnvCSVTable {
  name = "EnvCSVTable${Natural/show year}",
  dependsOn = [
    "PerPatSeriesToVector${Natural/show year}",
    "PerPatSeriesACS${Natural/show year}",
    "PerPatSeriesACS2${Natural/show year}",
    "PerPatSeriesNearestRoad${Natural/show year}",
    "PerPatSeriesNearestRoad2${Natural/show year}",
    "EnvDataSource${Natural/show year}"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocCSVTableConfig",
    arguments = {
      patient_file = "${basedir}/FHIR_processed/${Natural/show year}/PatVec",
      environment_file = "${basedir}/other_processed/${Natural/show year}/env",
      input_files = [
        acs year,
        acs2 year,
        nearestroad year,
	nearestroad2 year
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
     List/fold YearSkip skipList (List Step) (λ(yearSkip : YearSkip) → λ(stepList : List Step) → [
       fhirStep yearSkip.skip.fhir yearSkip.year yearSkip.resourceTypes yearSkip.skip_preproc,
       toVectorStep yearSkip.skip.toVector yearSkip.year,
       envDataSourceStep yearSkip.skip.envDataSource yearSkip.year,
       acsStep yearSkip.skip.acs yearSkip.year,
       acs2Step yearSkip.skip.acs2 yearSkip.year,
       nearestRoadStep yearSkip.skip.nearestRoad yearSkip.year,
       nearestRoad2Step yearSkip.skip.nearestRoad2 yearSkip.year,
       envCSVTableStep yearSkip.skip.envCSVTable yearSkip.year
     ] # stepList) ([] : List Step)
} : Config