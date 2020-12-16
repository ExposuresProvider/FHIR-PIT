let pipeline = ./pipeline.dhall

in pipeline "report" "progress" "/share/spark/hao/datatrans/spark/config" "/var/fhir" "/var/fhir" "/share/spark/hao/data" {
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
  yearStart = 2010,
  yearEnd = 2019
} [{
  year = 2010,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2011,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2012,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2013,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2014,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2015,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2016,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2017,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2018,
  skip = {
    csvTable = "run"
  }
}, {
  year = 2019,
  skip = {
    csvTable = "run"
  }
}]
