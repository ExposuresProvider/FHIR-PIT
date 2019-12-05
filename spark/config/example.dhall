let pipeline = ./pipeline.dhall

let resc_types1 = {
    Condition = "Condition",
    Lab = "Observation_Labs",
    Encounter = "Encounter",
    MedicationRequest = "MedicationRequest",
    Patient = "Patient",
    Procedure = "Procedure"
}

let resc_types2 = {
    Condition = "Condition",
    Lab = "Lab",
    Encounter = "Encounter",
    MedicationRequest = "MedicationRequest",
    Patient = "Patient",
    Procedure = "Procedure"
}

in pipeline "/share/spark/hao/data" "/share/spark/hao/data" "/share/spark/hao/data" [{
  year = 2012,
  skip = {
    fhir = True,
    toVector = True,
    envDataSource = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    envCSVTable = True
  },
  resourceTypes = resc_types1
}, {
  year = 2013,
  skip = {
    fhir = True,
    toVector = True,
    envDataSource = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    envCSVTable = True
  },
  resourceTypes = resc_types1
}, {
  year = 2014,
  skip = {
    fhir = True,
    toVector = True,
    envDataSource = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    envCSVTable = True
  },
  resourceTypes = resc_types1
}, {
  year = 2015,
  skip = {
    fhir = True,
    toVector = True,
    envDataSource = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    envCSVTable = True
  },
  resourceTypes = resc_types2
}, {
  year = 2016,
  skip = {
    fhir = True,
    toVector = True,
    envDataSource = True,
    acs = True,
    acs2 = True,
    nearestRoad = True,
    nearestRoad2 = True,
    envCSVTable = True
  },
  resourceTypes = resc_types2
}]
