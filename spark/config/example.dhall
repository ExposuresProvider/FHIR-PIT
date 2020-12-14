let pipeline = ./pipeline.dhall

in pipeline "report" "progress" "/share/spark/hao/datatrans/spark/config" "/var/fhir" "/var/fhir" "/share/spark/hao/data" {
  skip = {
    fhir = True,
    envDataCoordinates = True,
    latLonToGeoid = True,
    envDataFIPS = True,
    split = True,
    envDataAggregateCoordinates = True,
    envDataAggregateFIPS = True,
    acs = True,
    acsUR = True,
    nearestRoadTL = True,
    nearestRoadHPMS = True,
    cafo = True,
    landfill = True,
    toVector = True,
    perPatSeriesCSVTable = True
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
