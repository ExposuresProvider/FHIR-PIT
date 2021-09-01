let pipeline = ./pipeline.dhall

in pipeline "report" "progress" "/home/a/git/FHIR-PIT/spark/config" "/home/a/git/fhir_sample_data" "/home/a/fhir-pit/temp" "/home/a/fhir-pit/output" {
  skip = {
    mergeLocal = "run",
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
    binICEES = "run",
    binEPR = "run"
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
  end_date = "2020-01-01T00:00:00Z",
  study_period_splits = ["2011-01-01T00:00:00Z", "2012-01-01T00:00:00Z", "2013-01-01T00:00:00Z", "2014-01-01T00:00:00Z", "2015-01-01T00:00:00Z", "2016-01-01T00:00:00Z", "2017-01-01T00:00:00Z", "2018-01-01T00:00:00Z", "2019-01-01T00:00:00Z"],
  study_periods = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"],
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
}, {
  study_period = "2012",
  study_period_start = "2012-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2013",
  study_period_start = "2013-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2014",
  study_period_start = "2014-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2015",
  study_period_start = "2015-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2016",
  study_period_start = "2016-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2017",
  study_period_start = "2017-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2018",
  study_period_start = "2018-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}, {
  study_period = "2019",
  study_period_start = "2019-01-01T00:00:00Z",
  skip = {
    csvTable = "run"
  }
}] "/home/a/miniconda3/envs/sample/bin/python"
