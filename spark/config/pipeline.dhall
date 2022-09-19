let Prelude = ./dhall-lang/Prelude/package.dhall

let ResourceTypes : Type = {
    Condition: Text,
    Lab: Text,
    Encounter: Text,
    MedicationRequest: Text,
    Patient: Text,
    Procedure: Text
}

let StudyPeriodConfig : Type = {
    study_period : Text,
    study_period_start : Text,
    skip : {
      csvTable : Text
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
      mergeLocal: Text,
      fhir: Text,
      envDataCoordinates : Text,
      latLonToGeoid: Text,
      envDataFIPS : Text,
      split : Text,
      envDataAggregateFIPS : Text,
      envDataAggregateCoordinates : Text,
      acs : Text,
      acsUR : Text,
      nearestRoadTL : Text,
      nearestRoadHPMS : Text,
      cafo: Text,
      landfill: Text,
      toVector : Text,
      perPatSeriesCSVTable : Text,
      perPatSeriesCSVTableLocal : Text,
      addXWalkData: Text,
      binICEES: Text,
      binEPR: Text
    },
    skip_preproc: List Text,
    data_input: List (List Text),
    study_period_splits: List Text,
    study_periods: List Text,
    start_date: Text,
    end_date: Text,
    offset_hours: Integer
}

in λ(report : Text) → λ(progress : Text) → λ(configdir : Text) → λ(basedirinput : Text) → λ(basedir : Text) → λ(basediroutput : Text) → λ(fhirConfig : FhirConfig) → λ(skipList : List StudyPeriodConfig) →  λ(pyexec : Text) → 

let GenericStep : Type → Type = λ(a : Type) → {
    name : Text,
    dependsOn: List (List Text),
    skip : Text,
    step : {
        function : Text,
        arguments : a
    }
}

let SystemStep : Type = GenericStep {
    pyexec: Text,
    requirements: List Text,
    command: List Text,
    workdir: Text
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
    statistics: List Text,
    study_period_bounds: List Text,
    study_periods: List Text,
    offset_hours: Integer
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
    study_period_bounds : List Text,
    study_periods : List Text,
    offset_hours : Integer
}

let csvTableStep : Type = GenericStep {
    input_dir : Text,
    input_dir_patient : Text,
    output_dir : Text,
    deidentify : List Text,
    study_period_start : Text,
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
    PerPatSeriesCSVTable : PerPatSeriesCSVTableStep |
    System : SystemStep
>

let Config: Type = {
    report_output : Text,
    progress_output : Text,
    steps : List Step
}

let patgeo_output_path = "${basedir}/FHIR_processed/geo.csv"
let acs_output_path = "${basedir}/other_processed/acs.csv"
let acsUR_output_path = "${basedir}/other_processed/acsUR.csv"
let nearestRoadTL_output_path = "${basedir}/other_processed/nearestRoadTL.csv"
let nearestRoadHPMS_output_path = "${basedir}/other_processed/nearestRoadHPMS.csv"
let cafo_output_path = "${basedir}/other_processed/cafo.csv"
let landfill_output_path = "${basedir}/other_processed/landfill.csv"
let feature_map_path = "${configdir}/icees_features.yaml"
let requirements = [
    "isodate==0.6.0",
    "joblib==0.17.0",
    "numpy==1.19.4",
    "pandas==1.1.4",
    "parsedatetime==2.6",
    "progressbar2==3.53.1",
    "python-dateutil==2.8.1",
    "pytimeparse==1.1.8",
    "pytz==2020.4",
    "PyYAML==5.3.1",
    "text-unidecode==1.3",
    "tqdm==4.53.0",
    "tx-functional==0.1.2"
]

let mergeLocalStep = \(skip : Text) -> Step.System {
    name = "MergeLocal",
    dependsOn = [] : List (List Text),
    skip = skip,
    step = {
        function = "datatrans.step.PreprocSystem",
        arguments = {
            pyexec = pyexec,
            requirements = requirements,
            command = ["src/main/python/merge_fhir.py", "${basedirinput}/FHIR", "${basedir}/FHIR_merged"],
            workdir = "."
        }
    }
}

let fhirStep = λ(skip : Text) → λ(skip_preproc : List Text) → Step.FHIR {
    name = "FHIR",
    dependsOn = [["MergeLocal"]],
    skip = skip,
    step = {
        function = "datatrans.step.PreprocFHIR",
        arguments = {
            input_directory = "${basedir}/FHIR_merged",
            output_directory = "${basedir}/FHIR_processed",
            resc_types = resc_types,
            skip_preproc = skip_preproc
        }
    }
}

let toVectorStep = λ(skip : Text) → λ(start_date : Text) → λ(end_date : Text) → λ(offset_hours : Integer) → Step.ToVector {
  name = "PerPatSeriesToVector",
  dependsOn = [
    ["FHIR"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesToVector",
    arguments = {
      input_directory = "${basedir}/FHIR_processed/Patient",
      output_directory = "${basedir}/FHIR_vector",
      start_date = start_date,
      end_date = end_date,
      offset_hours = offset_hours,
      feature_map = feature_map_path
    }
  }
}

let envDataCoordinatesStep = λ(skip : Text) → λ(start_date : Text) → λ(end_date : Text) → λ(offset_hours : Integer) → Step.EnvDataCoordinates {
  name = "EnvDataCoordinates",
  dependsOn = [
    ["FHIR"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesEnvDataCoordinates",
    arguments = {
      patgeo_data = patgeo_output_path,
      environmental_data = "${basedirinput}/other/env",
      output_dir = "${basedir}/other_processed/env_coordinates",
      start_date = start_date,
      end_date = end_date,
      offset_hours = offset_hours
    }
  }
}

let latLonToGeoidStep = λ(skip : Text) → Step.LatLonToGeoid {
  name = "LatLonToGeoid",
  dependsOn = [
    ["FHIR"]
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

let envDataFIPSStep = λ(skip : Text) → λ(start_date : Text) → λ(end_date : Text) → λ(offset_hours : Integer) → Step.EnvDataFIPS {
  name = "EnvDataFIPS",
  dependsOn = [
    ["LatLonToGeoid"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataFIPS",
    arguments = {
      environmental_data = "${basedirinput}/other/env",
      fips_data = "${basedir}/other_processed/lat_lon_to_geoid/geoids.csv",
      output_file = "${basedir}/other_processed/env_FIPS/preagg",
      start_date = start_date,
      end_date = end_date,
      offset_hours = offset_hours
    }
  }
}

let splitStep = λ(skip : Text) → Step.Split {
  name = "Split",
  dependsOn = [
    ["EnvDataFIPS"]
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


let envDataAggregateCoordinatesStep = λ(skip : Text) → \(study_period_bounds : List Text) -> \(study_periods : List Text) -> \(offset_hours : Integer) -> Step.EnvDataAggregate {
  name = "EnvDataAggregateCoordinates",
  dependsOn = [
    ["EnvDataCoordinates"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataAggregate",
    arguments = {
      input_dir = "${basedir}/other_processed/env_coordinates",
      output_dir = "${basedir}/other_processed/env_agg_coordinates",
      indices = indices,
      statistics = statistics,
      study_period_bounds = study_period_bounds,
      study_periods = study_periods,
      offset_hours = offset_hours
    }
  }
}

let envDataAggregateFIPSStep = λ(skip : Text) → \(study_period_bounds : List Text) -> \(study_periods : List Text) -> \(offset_hours : Integer) → Step.EnvDataAggregate {
  name = "EnvDataAggregateFIPS",
  dependsOn = [
    ["Split"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocEnvDataAggregate",
    arguments = {
      input_dir = "${basedir}/other_processed/env_split_FIPS",
      output_dir = "${basedir}/other_processed/env_agg_FIPS",
      indices = indices2,
      statistics = statistics,
      study_period_bounds = study_period_bounds,
      study_periods = study_periods,
      offset_hours = offset_hours
    }
  }
}

let acsStep = λ(skip : Text) → Step.ACS {
  name = "PerPatSeriesACS",
  dependsOn = [
    ["FHIR"]
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
      feature_map = feature_map_path
    }
  }
}

let acsURStep = λ(skip : Text) → Step.ACS {
  name = "PerPatSeriesACSUR",
  dependsOn = [
    ["FHIR"]
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
      feature_map = feature_map_path
    }
  }
}

let nearestRoadTLStep = λ(skip : Text) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoadTL",
  dependsOn = [
    ["FHIR"]
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
      feature_map = feature_map_path
    }
  }
}

let nearestRoadHPMSStep = λ(skip : Text) → Step.NearestRoad {
  name = "PerPatSeriesNearestRoadHPMS",
  dependsOn = [
    ["FHIR"]
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
      feature_map = feature_map_path
    }
  }
}

let cafoStep = λ(skip : Text) → Step.NearestPoint {
  name = "PerPatSeriesCAFO",
  dependsOn = [
    ["FHIR"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestPoint",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestpoint_data = "${basedirinput}/other/spatial/BDT_PointDatasets/Permitted_Animal_Facilities-4-1-2020.shp",
      output_file = cafo_output_path,
      feature_name = "cafo",
      feature_map = feature_map_path
    }
  }
}

let landfillStep = λ(skip : Text) → Step.NearestPoint {
  name = "PerPatSeriesLandfill",
  dependsOn = [
    ["FHIR"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesNearestPoint",
    arguments = {
      patgeo_data = patgeo_output_path,
      nearestpoint_data = "${basedirinput}/other/spatial/BDT_PointDatasets/Active_Permitted_Landfills_geo.shp",
      output_file = landfill_output_path,
      feature_name = "landfill",
      feature_map = feature_map_path
    }
  }
}

let perPatSeriesCSVTableStep = λ(skip : Text) → λ(study_period_bounds : List Text) → λ(study_periods : List Text) → λ(offset_hours : Integer) → \(dependencies: List (List Text)) -> Step.PerPatSeriesCSVTable {
  name = "PerPatSeriesCSVTable",
  dependsOn = dependencies,
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesCSVTable",
    arguments = {
      patient_file = "${basedir}/FHIR_vector/visit",
      environment_file = "${basedir}/other_processed/env_agg_coordinates",
      environment2_file = "${basedir}/other_processed/env_agg_FIPS",
      input_files = [
        acs_output_path,
        acsUR_output_path,
        nearestRoadTL_output_path,
        nearestRoadHPMS_output_path,
	    cafo_output_path,
	    landfill_output_path
      ],
      output_dir = "${basedir}/icees",
      study_period_bounds = study_period_bounds,
      study_periods = study_periods,
      offset_hours = offset_hours
    }
  }
}

let perPatSeriesCSVTableLocalStep = λ(skip : Text) → λ(study_period_bounds : List Text) → λ(study_periods : List Text) → λ(offset_hours : Integer) → λ(dependencies: List (List Text)) → Step.PerPatSeriesCSVTable {
  name = "PerPatSeriesCSVTableLocal",
  dependsOn = dependencies,
  skip = skip,
  step = {
    function = "datatrans.step.PreprocPerPatSeriesCSVTableLocal",
    arguments = {
      patient_file = "${basedir}/FHIR_vector/visit",
      environment_file = "${basedir}/other_processed/env_agg_coordinates",
      environment2_file = "${basedir}/other_processed/env_agg_FIPS",
      input_files = [
        acs_output_path,
        acsUR_output_path,
        nearestRoadTL_output_path,
        nearestRoadHPMS_output_path,
	    cafo_output_path,
	    landfill_output_path
      ],
      output_dir = "${basedir}/icees",
      study_period_bounds = study_period_bounds,
      study_periods = study_periods,
      offset_hours = offset_hours
    }
  }
}

let csvTableStep = λ(skip : Text) → λ(study_period_start : Text) → λ(study_period : Text) → λ(offset_hours : Integer) → Step.csvTable {
  name = "csvTable${study_period}",
  dependsOn = [
    ["PerPatSeriesCSVTable", "PerPatSeriesCSVTableLocal"]
  ],
  skip = skip,
  step = {
    function = "datatrans.step.PreprocCSVTable",
    arguments = {
      input_dir = "${basedir}/icees/${study_period}/per_patient",
      input_dir_patient = "${basedir}/FHIR_Vector/patient",
      output_dir = "${basedir}/icees2/${study_period}",
      deidentify = [] : List Text,
      study_period_start = study_period_start,
      offset_hours = offset_hours,
      feature_map = feature_map_path
    }
  }
}

let addXWalkDataStep = \(skip : Text) -> \(study_periods : List Text) -> Step.System {
    name = "addXWalkData",
    dependsOn = Prelude.List.map Text (List Text) (\(enumeration : Text) -> ["csvTable${enumeration}"]) study_periods,
    skip = skip,
    step = {
        function = "datatrans.step.PreprocSystem",
        arguments = {
            pyexec = pyexec,
            requirements = requirements,
            command = ["src/main/python/addXWalkData.py", "${basedirinput}/ICEESPCD/8000PtsXWalkForHao.csv", "${basedirinput}/ICEESPCD/RegistryPtsXWalkForHao.csv", "${basedir}/icees2", "${basediroutput}/icees2_xwalk"] # study_periods,
            workdir = "."
        }
    }
}

let binICEESStep = \(skip : Text) -> \(study_periods : List Text) -> Step.System {
    name = "BinICEES",
    dependsOn = Prelude.List.map Text (List Text) (\(enumeration : Text) -> ["csvTable${enumeration}"]) study_periods,
    skip = skip,
    step = {
        function = "datatrans.step.PreprocSystem",
        arguments = {
            pyexec = pyexec,
            requirements = requirements,
            command = ["src/main/python/preprocBinning.py", feature_map_path, "${basedir}/icees2_xwalk", "${basediroutput}/icees2_bins"] # study_periods,
            workdir = "."
        }
    }
}

let binEPRStep = \(skip : Text) -> \(study_periods : List Text) -> Step.System {
    name = "BinEPR",
    dependsOn = [["BinICEES"]],
    skip = skip,
    step = {
        function = "datatrans.step.PreprocSystem",
        arguments = {
            pyexec = pyexec,
            requirements = requirements,
            command = ["src/main/python/binEPR.py", "${basedirinput}/EPR/TLR4_AllData_NewHash_01292020 NO PII_no_new_line.csv", "${basedirinput}/EPR/UNC_NIEHS_XWalk_for_Hao_shape_h3.csv", "${basediroutput}/icees2_bins/", "${basedir}/FHIR_processed/geo.csv", "${basediroutput}/EPR_binned/EPR_binned"] # study_periods,
            workdir = "."
        }
    }
}

let study_period_bounds = [fhirConfig.start_date] # fhirConfig.study_period_splits # [fhirConfig.end_date]

in {
  report_output = report,
  progress_output = progress,
  steps =
    [
      mergeLocalStep fhirConfig.skip.mergeLocal,
      fhirStep fhirConfig.skip.fhir fhirConfig.skip_preproc,
      envDataCoordinatesStep fhirConfig.skip.envDataCoordinates fhirConfig.start_date fhirConfig.end_date fhirConfig.offset_hours,
      latLonToGeoidStep fhirConfig.skip.latLonToGeoid,
      envDataFIPSStep fhirConfig.skip.envDataFIPS fhirConfig.start_date fhirConfig.end_date fhirConfig.offset_hours,
      splitStep fhirConfig.skip.split,
      envDataAggregateCoordinatesStep fhirConfig.skip.envDataAggregateCoordinates study_period_bounds fhirConfig.study_periods fhirConfig.offset_hours,
      envDataAggregateFIPSStep fhirConfig.skip.envDataAggregateFIPS study_period_bounds fhirConfig.study_periods fhirConfig.offset_hours,
      acsStep fhirConfig.skip.acs,
      acsURStep fhirConfig.skip.acsUR,
      nearestRoadTLStep fhirConfig.skip.nearestRoadTL,
      nearestRoadHPMSStep fhirConfig.skip.nearestRoadHPMS,
      cafoStep fhirConfig.skip.cafo,
      landfillStep fhirConfig.skip.landfill,
      toVectorStep fhirConfig.skip.toVector fhirConfig.start_date fhirConfig.end_date fhirConfig.offset_hours,
      perPatSeriesCSVTableStep fhirConfig.skip.perPatSeriesCSVTable study_period_bounds fhirConfig.study_periods fhirConfig.offset_hours fhirConfig.data_input,
      perPatSeriesCSVTableLocalStep fhirConfig.skip.perPatSeriesCSVTableLocal study_period_bounds fhirConfig.study_periods fhirConfig.offset_hours fhirConfig.data_input,
      addXWalkDataStep fhirConfig.skip.addXWalkData fhirConfig.study_periods,
      binICEESStep fhirConfig.skip.binICEES fhirConfig.study_periods,
      binEPRStep fhirConfig.skip.binEPR fhirConfig.study_periods
    ] # Prelude.List.map StudyPeriodConfig Step (\(study_period_skip : StudyPeriodConfig) -> csvTableStep study_period_skip.skip.csvTable study_period_skip.study_period_start study_period_skip.study_period fhirConfig.offset_hours) skipList
} : Config
