let pipeline = ./pipeline_demo.dhall
let basedir = "/home/jjgarcia/projects/demo-FHIRPIT/FHIR-PIT" 

in pipeline "report" "progress" "${basedir}/spark/config" "${basedir}/data/input" "${basedir}/data/output" "${basedir}/data/output" {
  skip = {
    mergeLocal = "skip",
    fhir = "run",
    envDataCoordinates = "run",
    latLonToGeoid = "skip",
    envDataFIPS = "skip",
    split = "skip",
    envDataAggregateCoordinates = "run",
    envDataAggregateFIPS = "skip",
    acs = "run",
    acsUR = "run",
    nearestRoadTL = "skip",
    nearestRoadHPMS = "skip",
    cafo = "run",
    landfill = "run",
    toVector = "run",
    perPatSeriesCSVTable = "run",
    perPatSeriesCSVTableLocal = "skip",
    addXWalkData = "skip",
    binICEES = "run",
    binEPR = "skip"
  },
  skip_preproc = [] : List Text,
  data_input = [
    ["PerPatSeriesToVector"],
    ["PerPatSeriesACS"],
    ["PerPatSeriesACSUR"],
    ["PerPatSeriesNearestRoadTL"],
    ["PerPatSeriesNearestRoadHPMS"],
    ["PerPatSeriesCAFO"],
    ["PerPatSeriesLandfill"],
    ["EnvDataAggregateCoordinates"],
    ["EnvDataAggregateFIPS"]
  ],
  start_date = "2010-01-01T00:00:00Z",
  end_date = "2012-01-01T00:00:00Z",
  study_period_splits = ["2011-01-01T00:00:00Z"],
  study_periods = ["2010", "2011"],
  offset_hours = -5
} [{
  study_period = "2010",
  study_period_start = "2010-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2011",
  study_period_start = "2011-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}] "python"

