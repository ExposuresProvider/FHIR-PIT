let pipeline = ./pipeline_demo.dhall
let basedir = "/FHIR-PIT"
let python_exec = "/FHIR-PIT/fhir-pit/bin/python"

in pipeline "report" "progress" "${basedir}/spark/config" "${basedir}/data/input" "${basedir}/data/output" "${basedir}/data/output" {
  skip = {
  mergeLocal = "skip",
    fhir = "run",
    envDataCoordinates = "run",
    latLonToGeoid = "run",
    envDataFIPS = "run",
    split = "run",
    envDataAggregateCoordinates = "run",
    envDataAggregateFIPS = "run",
    acs = "run",
    acsUR = "run",
    nearestRoadTL = "run",
    nearestRoadHPMS = "run",
    cafo = "run",
    landfill = "run",
    toVector = "run",
    perPatSeriesCSVTable = "skip",
    perPatSeriesCSVTableLocal = "run",
    deidentify = "run",
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
}] "${python_exec}"

