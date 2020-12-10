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
      csvTable : Bool
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
      envDataCoordinates : Bool,
      latLonToGeoid: Bool,
      envDataFIPS : Bool,
      split : Bool,
      envDataAggregateFIPS : Bool,
      envDataAggregateCoordinates : Bool,
      acs : Bool,
      acsUR : Bool,
      nearestRoadTL : Bool,
      nearestRoadHPMS : Bool,
      cafo: Bool,
      landfill: Bool,
      toVector : Bool,
      perPatSeriesCSVTable : Bool
    },
    skip_preproc: List Text,
    yearStart: Natural,
    yearEnd: Natural
}

in λ(report : Text) → λ(progress : Text) → λ(configdir : Text) → λ(basedirinput : Text) → λ(basedir : Text) → λ(basediroutput : Text) → λ(fhirConfig : FhirConfig) → λ(skipList : List YearConfig) →

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
    feature_map: Text
}

let EnvDataCoordinatesStep : Type = GenericStep {
    patgeo_data: Text,
    environmental_data: Text,
    output_dir: Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer
}

let LatLonToGeoidStep : Type = GenericStep {
    patgeo_data: Text,
    fips_data: Text,
    output_file: Text
}

let EnvDataFIPSStep : Type = GenericStep {
    environmental_data: Text,
    fips_data: Text,
    output_file: Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer
}

let SplitStep : Type = GenericStep {
    input_file: Text,
    split_index: Text,
    output_dir: Text
}

let EnvDataAggregateStep : Type = GenericStep {
    input_dir: Text,
    output_dir: Text,
    indices: List Text,
    statistics: List Text
}

let PerPatSeriesACSStep : Type = GenericStep {
    time_series : Text,
    acs_data : Text,
    geoid_data : Text,
    output_file : Text,
    feature_map : Text,
    feature_name : Text
}

let PerPatSeriesNearestRoadStep : Type = GenericStep {
    patgeo_data : Text,
    nearestroad_data : Text,
    maximum_search_radius : Double,
    output_file : Text,
    feature_map : Text,
    feature_name : Text
}

let PerPatSeriesNearestPointStep : Type = GenericStep {
    patgeo_data : Text,
    nearestpoint_data : Text,
    output_file : Text,
    feature_map : Text,
    feature_name : Text
}

let PerPatSeriesCSVTableStep : Type = GenericStep {
    patient_file : Text,
    environment_file : Text,
    environment2_file : Text,
    input_files : List Text,
    output_dir : Text,
    start_date : Text,
    end_date : Text,
    offset_hours : Integer
}

let csvTableStep : Type = GenericStep {
    input_dir : Text,
    output_dir : Text,
    deidentify : List Text,
    offset_hours : Integer,
    feature_map: Text
}

let Step : Type = <
    FHIR : FHIRStep |
    ToVector : PerPatSeriesToVectorStep |
    EnvDataCoordinates : EnvDataCoordinatesStep |
    LatLonToGeoid : LatLonToGeoidStep |
    EnvDataFIPS : EnvDataFIPSStep |
    Split : SplitStep |
    EnvDataAggregate : EnvDataAggregateStep |
    ACS : PerPatSeriesACSStep |
    NearestRoad : PerPatSeriesNearestRoadStep |
    NearestPoint : PerPatSeriesNearestPointStep |
    csvTable : csvTableStep |
    PerPatSeriesCSVTable : PerPatSeriesCSVTableStep
>

let Config: Type = {
    report_output : Text,
    progress_output : Text,
    steps : List Step
}

let start_year = λ(year : Natural) → "${Natural/show year}-01-01T00:00:00-05:00"
let end_year = λ(year : Natural) → start_year (year + 1)
let patgeo_output_path = "${basedir}/FHIR_processed/geo.csv"
let acs_output_path = "${basedir}/other_processed/acs.csv"
let acsUR_output_path = "${basedir}/other_processed/acsUR.csv"
let nearestRoadTL_output_path = "${basedir}/other_processed/nearestRoadTL.csv"
let nearestRoadHPMS_output_path = "${basedir}/other_processed/nearestRoadHPMS.csv"
let cafo_output_path = "${basedir}/other_processed/cafo.csv"
let landfill_output_path = "${basedir}/other_processed/landfill.csv"

let fhirStep = λ(skip : Bool) → λ(skip_preproc : List Text) → Step.FHIR {
    name = "FHIR",
    dependsOn = [] : List Text,
    skip = skip,
    step = {
        function = "datatrans.step.PreprocFHIR",
        arguments = {
            input_directory = "${basedirinput}/FHIR_merged",
            output_directory = "${basedir}/FHIR_processed",
            resc_types = resc_types,
            skip_preproc = skip_preproc
        }
    }
}

let toVectorStep = λ(skip : Bool) → Step.ToVector {
  name = "PerPatSeriesToVector",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesToVector",
    arguments = {
      input_directory = "${basedir}/FHIR_processed/Patient",
      output_directory = "${basedir}/FHIR_vector",
      start_date = start_year 2010,
      end_date = end_year 2019,
      offset_hours = -5,
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let envDataCoordinatesStep = λ(skip : Bool) → λ(year_start : Natural) → λ(year_end : Natural) → Step.EnvDataCoordinates {
  name = "EnvDataCoordinates",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesEnvDataCoordinates",
    arguments = {
      patgeo_data = patgeo_output_path,
      environmental_data = "${basedirinput}/other/env",
      output_dir = "${basedir}/other_processed/env",
      start_date = start_year year_start,
      end_date = end_year year_end,
      offset_hours = -5
    }
  }
}

let latLonToGeoidStep = λ(skip : Bool) → Step.LatLonToGeoid {
  name = "LatLonToGeoid",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocLatLonToGeoid",
    arguments = {
      patgeo_data = patgeo_output_path,
      fips_data = "${basedirinput}/other/spatial/env/US_Census_Tracts_LCC/US_Census_Tracts_LCC.shp",
      output_file = "${basedir}/other_processed/lat_lon_to_geoid/geoids.csv"
    }
  }
}

let statistics = [
  "max",
  "avg"
]

let envDataFIPSStep = λ(skip : Bool) → λ(year_start : Natural) → λ(year_end : Natural) → Step.EnvDataFIPS {
  name = "EnvDataFIPS",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataFIPS",
    arguments = {
      environmental_data = "${basedirinput}/other/env",
      fips_data = "${basedir}/other_processed/lat_lon_to_geoid/geoids.csv",
      output_file = "${basedir}/other_processed/env_FIPS/preagg",
      start_date = start_year year_start,
      end_date = end_year year_end,
      offset_hours = -5
    }
  }
}

let splitStep = λ(skip : Bool) → Step.Split {
  name = "Split",
  dependsOn = [
    "EnvDataFIPS"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocSplit",
    arguments = {
      input_file = "${basedir}/other_processed/env_FIPS/preagg",
      split_index = "patient_num",
      output_dir = "${basedir}/other_processed/env_split_FIPS"
    }
  }
}

let indices = ["pm25_max", "pm25_avg", "o3_max", "o3_avg"]

let indices2 = [
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
]

let statistics = ["avg", "max"]


let envDataAggregateCoordinatesStep = λ(skip : Bool) → Step.EnvDataAggregate {
  name = "EnvDataAggregateCoordinates",
  dependsOn = [
    "EnvDataCoordinates"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataAggregate",
    arguments = {
      input_dir = "${basedir}/other_processed/env_coordinates",
      output_dir = "${basedir}/other_processed/env_agg_coordinates",
      indices = indices,
      statistics = statistics
    }
  }
}

let envDataAggregateFIPSStep = λ(skip : Bool) → Step.EnvDataAggregate {
  name = "EnvDataAggregateFIPS",
  dependsOn = [
    "EnvDataFIPS"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataAggregate",
    arguments = {
      input_dir = "${basedir}/other_processed/env_split_FIPS",
      output_dir = "${basedir}/other_processed/env_agg_FIPS",
      indices = indices2,
      statistics = statistics
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
    function = "datatrans.step.PreprocPerPatSeriesACS",
    arguments = {
      time_series = patgeo_output_path,
      acs_data = "${basedirinput}/other/spatial/acs/ACS_NC_2016_with_column_headers.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acs_output_path,
      feature_name = "acs",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let acsURStep = λ(skip : Bool) → Step.ACS {
  name = "PerPatSeriesACSUR",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesACS",
    arguments = {
      time_series = patgeo_output_path,
      acs_data = "${basedirinput}/other/spatial/acs/Appold_trans_geo_cross_02.10.10 - trans_geo_cross.csv",
      geoid_data = "${basedirinput}/other/spatial/acs/tl_2016_37_bg_lcc.shp",
      output_file = acsUR_output_path,
      feature_name = "acsUR",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }                                                                                                                                    }

let nearestRoadTLStep = λ(skip : Bool) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoadTL",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoad",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestroad_data = "${basedirinput}/other/spatial/nearestRoadTL/tl_2015_allstates_prisecroads_lcc.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestRoadTL_output_path,
      feature_name = "nearestRoadTL",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let nearestRoadHPMSStep = λ(skip : Bool) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoadHPMS",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestRoad",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestroad_data = "${basedirinput}/other/spatial/nearestRoadHPMS/hpms2016_major_roads_lcc.shp",
      maximum_search_radius = Integer/toDouble (Natural/toInteger 500),
      output_file = nearestRoadHPMS_output_path,
      feature_name = "nearestRoadHPMS",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let cafoStep = λ(skip : Bool) → Step.NearestPoint {
  name = "PerPatSeriesCAFO",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestPoint",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestpoint_data = "${basedirinput}/other/spatial/BDT_PointDatasets/Permitted_Animal_Facilities-4-1-2020.shp",
      output_file = cafo_output_path,
      feature_name = "cafo",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let landfillStep = λ(skip : Bool) → Step.NearestPoint {
  name = "PerPatSeriesLandfill",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestPoint",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestpoint_data = "${basedirinput}/other/spatial/BDT_PointDatasets/Active_Permitted_Landfills_geo.shp",
      output_file = landfill_output_path,
      feature_name = "landfill",
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

let perPatSeriesCSVTableStep = λ(skip : Bool) →  λ(year_start : Natural) →  λ(year_end : Natural) →  Step.PerPatSeriesCSVTable {
  name = "PerPatSeriesCSVTable",
  dependsOn = [
    "PerPatSeriesToVector",
    "PerPatSeriesACS",
    "PerPatSeriesACSUR",
    "PerPatSeriesNearestRoadTL",
    "PerPatSeriesNearestRoadHPMS",
    "PerPatSeriesCAFO",
    "PerPatSeriesLandfill",
    "EnvDataAggregateCoordinates",
    "EnvDataAggregateFIPS"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesCSVTable",
    arguments = {
      patient_file = "${basedir}/FHIR_vector",
      environment_file = "${basedir}/other_processed/env6",
      environment2_file = "${basedir}/other_processed/env5",
      input_files = [
        acs_output_path,
        acsUR_output_path,
        nearestRoadTL_output_path,
        nearestRoadHPMS_output_path,
	cafo_output_path,
	landfill_output_path
      ],
      output_dir = "${basedir}/icees",
      start_date = start_year year_start,
      end_date = end_year year_end,
      offset_hours = -5
    }
  }
}

let csvTableStep = λ(skip : Bool) → λ(year : Natural) → Step.csvTable {
  name = "csvTable${Natural/show year}",
  dependsOn = [
    "PerPatSeriesCSVTable"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocCSVTable",
    arguments = {
      input_dir = "${basedir}/icees/${Natural/show year}/per_patient",
      output_dir = "${basediroutput}/icees2/${Natural/show year}",
      deidentify = [] : List Text,
      offset_hours = -5,
      feature_map = "${configdir}/icees_features.yaml"
    }
  }
}

in {
  report_output = report,
  progress_output = progress,
  steps =
    [
      fhirStep fhirConfig.skip.fhir fhirConfig.skip_preproc,
      envDataCoordinatesStep fhirConfig.skip.envDataCoordinates fhirConfig.yearStart fhirConfig.yearEnd,
      latLonToGeoidStep fhirConfig.skip.latLonToGeoid,
      envDataFIPSStep fhirConfig.skip.envDataFIPS fhirConfig.yearStart fhirConfig.yearEnd,
      splitStep fhirConfig.skip.split,
      envDataAggregateCoordinatesStep fhirConfig.skip.envDataAggregateCoordinates,
      envDataAggregateFIPSStep fhirConfig.skip.envDataAggregateFIPS,
      acsStep fhirConfig.skip.acs,
      acsURStep fhirConfig.skip.acsUR,
      nearestRoadTLStep fhirConfig.skip.nearestRoadTL,
      nearestRoadHPMSStep fhirConfig.skip.nearestRoadHPMS,
      cafoStep fhirConfig.skip.cafo,
      landfillStep fhirConfig.skip.landfill,
      toVectorStep fhirConfig.skip.toVector,
      perPatSeriesCSVTableStep fhirConfig.skip.perPatSeriesCSVTable fhirConfig.yearStart fhirConfig.yearEnd
    ] # List/fold YearConfig skipList (List Step) (λ(yearSkip : YearConfig) → λ(stepList : List Step) → [
      csvTableStep yearSkip.skip.csvTable yearSkip.year
    ] # stepList) ([] : List Step)
} : Config
