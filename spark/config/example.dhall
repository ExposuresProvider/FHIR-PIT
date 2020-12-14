let pipeline = ./pipeline.dhall

in pipeline "report" "progress" "/share/spark/hao/datatrans/spark/config" "/var/fhir" "/var/fhir" "/share/spark/hao/data" {
  skip = {
    mergeLocal = "reuse",
    fhir = "reuse",
    envDataCoordinates = "reuse",
    latLonToGeoid = "reuse",
    envDataFIPS = "reuse",
    split = "reuse",
    envDataAggregateCoordinates = "reuse",
    envDataAggregateFIPS = "reuse",
    acs = "reuse",
    acsUR = "reuse",
    nearestRoadTL = "reuse",
    nearestRoadHPMS = "reuse",
    cafo = "reuse",
    landfill = "reuse",
    toVector = "reuse",
    perPatSeriesCSVTable = "skip",
    perPatSeriesCSVTableLocal = "reuse",
    binICEES = "run",
    binEPR = "run"
  },
  skip_preproc = [] : List Text,
  yearStart = 2010,
  yearEnd = 2019
} [{
  year = 2010,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2011,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2012,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2013,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2014,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2015,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2016,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2017,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2018,
  skip = {
    csvTable = "reuse"
  }
}, {
  year = 2019,
  skip = {
    csvTable = "reuse"
  }
}]
