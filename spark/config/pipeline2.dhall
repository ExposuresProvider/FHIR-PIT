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
      fips: Bool,
      envDataFIPS : Bool,
      split : Bool,
      envDataAggregate : Bool,
      acs : Bool,
      acs2 : Bool,
      nearestRoad : Bool,
      nearestRoad2 : Bool,
      toVector : Bool,
      perPatSeriesCSVTable : Bool
    },
    skip_preproc: List Text,
    yearStart: Natural,
    yearEnd: Natural
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

let EnvDataCoordinatesStep : Type = GenericStep {
    patgeo_data: Text,
    environmental_data: Text,
    output_dir: Text,
    indices: List Text,
    statistics: List Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer
}

let FIPSStep : Type = GenericStep {
    patgeo_data: Text,
    fips_data: Text,
    output_file: Text
}

let EnvDataFIPSStep : Type = GenericStep {
    environmental_data: Text,
    fips_data: Text,
    output_file: Text,
    indices: List Text,
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
    statistics: List Text,
    indices: List Text
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
    start_date : Text,
    end_date : Text,
    deidentify : List Text,
    offset_hours : Integer
}

let Step : Type = <
    FHIR : FHIRStep |
    ToVector : PerPatSeriesToVectorStep |
    EnvDataCoordinates : EnvDataCoordinatesStep |
    FIPS : FIPSStep |
    EnvDataFIPS : EnvDataFIPSStep |
    Split : SplitStep |
    EnvDataAggregate : EnvDataAggregateStep |
    ACS : PerPatSeriesACSStep |
    NearestRoad : PerPatSeriesNearestRoadStep |
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

let toVectorStep = λ(skip : Bool) → Step.ToVector {
  name = "PerPatSeriesToVector",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesToVectorConfig",
    arguments = {
      input_directory = "${basedir}/FHIR_processed/Patient",
      output_directory = "${basedir}/FHIR_vector",
      start_date = start_year 2010,
      end_date = end_year 2016,
      offset_hours = -5,
      med_map = "${basedir}/other/medical/icees_features_rxnorm.json"
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
    function = "datatrans.step.EnvDataCoordinatesConfig",
    arguments = {
      patgeo_data = patgeo,
      environmental_data = "${basedirinput}/other/env",
      output_dir = "${basedir}/other_processed/env",
      indices = [] : List Text,
      statistics = [] : List Text,
      start_date = start_year year_start,
      end_date = end_year year_end,
      offset_hours = -5
    }
  }
}

let fipsStep = λ(skip : Bool) → Step.FIPS {
  name = "FIPS",
  dependsOn = [
    "FHIR"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.FIPSConfig",
    arguments = {
      patgeo_data = patgeo,
      fips_data = "${basedirinput}/other/spatial/env/US_Census_Tracts_LCC/US_Census_Tracts_LCC.shp",
      output_file = "${basedir}/other_processed/env2/geoids.csv"
    }
  }
}

let indices = [
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
]

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
    function = "datatrans.step.EnvDataFIPSConfig",
    arguments = {
      environmental_data = "${basedirinput}/other/env",
      fips_data = "${basedir}/other_processed/env2/geoids.csv",
      output_file = "${basedir}/other_processed/env3/preagg",
      indices = indices,
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
    function = "datatrans.step.SplitConfig",
    arguments = {
      input_file = "${basedir}/other_processed/env3/preagg",
      split_index = "patient_num",
      output_dir = "${basedir}/other_processed/env4"
    }
  }
}

let envDataAggregateStep = λ(skip : Bool) → Step.EnvDataAggregate {
  name = "EnvDataAggregate",
  dependsOn = [
    "EnvDataFIPS"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.EnvDataAggregateConfig",
    arguments = {
      input_dir = "${basedir}/other_processed/env4",
      output_dir = "${basedir}/other_processed/env5",
      statistics = statistics,
      indices = indices
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
  }                                                                                                                                    }

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

let perPatSeriesCSVTableStep = λ(skip : Bool) →  λ(year_start : Natural) →  λ(year_end : Natural) →  Step.PerPatSeriesCSVTable {
  name = "PerPatSeriesCSVTable",
  dependsOn = [
    "PerPatSeriesToVector",
    "PerPatSeriesACS",
    "PerPatSeriesACS2",
    "PerPatSeriesNearestRoad",
    "PerPatSeriesNearestRoad2",
    "EnvDataCoordinates",
    "EnvDataAggregate"
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesCSVTableConfig",
    arguments = {
      patient_file = "${basedir}/FHIR_vector",
      environment_file = "${basedir}/other_processed/env",
      environment2_file = "${basedir}/other_processed/env5",
      input_files = [
        acs,
        acs2,
        nearestroad,
        nearestroad2
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
    function = "datatrans.step.PreprocCSVTableConfig",
    arguments = {
      input_dir = "${basedir}/icees",
      output_dir = "${basediroutput}/icees2/${Natural/show year}",
      start_date = start_year year,
      end_date = end_year year,
      deidentify = [] : List Text,
      offset_hours = -5
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
      fipsStep fhirConfig.skip.fips,
      envDataFIPSStep fhirConfig.skip.envDataFIPS fhirConfig.yearStart fhirConfig.yearEnd,
      splitStep fhirConfig.skip.split,
      envDataAggregateStep fhirConfig.skip.envDataAggregate,
      acsStep fhirConfig.skip.acs,
      acs2Step fhirConfig.skip.acs2,
      nearestRoadStep fhirConfig.skip.nearestRoad,
      nearestRoad2Step fhirConfig.skip.nearestRoad2,
      toVectorStep fhirConfig.skip.toVector,
      perPatSeriesCSVTableStep fhirConfig.skip.perPatSeriesCSVTable fhirConfig.yearStart fhirConfig.yearEnd
    ] # List/fold YearConfig skipList (List Step) (λ(yearSkip : YearConfig) → λ(stepList : List Step) → [
      csvTableStep yearSkip.skip.csvTable yearSkip.year
    ] # stepList) ([] : List Step)
} : Config
