let pipeline = ./pipeline2.dhall

in pipeline "report" "progress" "/var/fhir" "/var/fhir" "/share/spark/hao/data" {
  skip = {
    fhir = True,
    fips = True,
    envDataFIPS = True,
    split = True,
    envDataAggregate = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True
  },
  skip_preproc = [] : List Text,
  yearStart = 2010,
  yearEnd = 2016
} [{
  year = 2010,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2011,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2012,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2013,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2014,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2015,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}, {
  year = 2016,
  skip = {
    toVector = True,
    envDataCoordinates = True,
    envCSVTable = True
  }
}]
