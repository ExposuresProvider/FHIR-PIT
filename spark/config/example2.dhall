let pipeline = ./pipeline2.dhall

in pipeline "report" "progress" "/var/fhir" "/var/fhir" "/share/spark/hao/data" {
  skip = {
    fhir = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True
  },
  skip_preproc = [] : List Text
} [{
  year = 2012,
  skip = {
    toVector = True,
    envDataSource = True,
    envCSVTable = True
  }
}, {
  year = 2013,
  skip = {
    toVector = True,
    envDataSource = True,
    envCSVTable = True
  }
}, {
  year = 2014,
  skip = {
    toVector = True,
    envDataSource = True,
    envCSVTable = True
  }
}, {
  year = 2015,
  skip = {
    toVector = True,
    envDataSource = True,
    envCSVTable = True
  }
}, {
  year = 2016,
  skip = {
    toVector = True,
    envDataSource = True,
    envCSVTable = True
  }
}]
