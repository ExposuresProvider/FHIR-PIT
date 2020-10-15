let pipeline = ./pipeline2.dhall

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
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    toVector = True,
    perPatSeriesCSVTable = True
  },
  skip_preproc = [] : List Text,
  yearStart = 2010,
  yearEnd = 2016
} [{
  year = 2010,
  skip = {
    csvTable = True
  }
}, {
  year = 2011,
  skip = {
    csvTable = True
  }
}, {
  year = 2012,
  skip = {
    csvTable = True
  }
}, {
  year = 2013,
  skip = {
    csvTable = True
  }
}, {
  year = 2014,
  skip = {
    csvTable = True
  }
}, {
  year = 2015,
  skip = {
    csvTable = True
  }
}, {
  year = 2016,
  skip = {
    csvTable = True
  }
}]
