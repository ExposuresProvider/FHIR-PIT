let pipeline = ./pipeline.dhall

in pipeline "report" "progress" "/share/spark/hao/datatrans/spark/config/COVID" "/var/fhir/COVID" "/share/spark/hao/data/COVID" "/share/spark/hao/data/COVID" {
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
    ["EnvDataAggregateCoordinates"],
    ["EnvDataAggregateFIPS"],       
    ["PerPatSeriesToVector"],
    ["PerPatSeriesACS"],
    ["PerPatSeriesACSUR"],
    ["PerPatSeriesNearestRoadTL"],
    ["PerPatSeriesNearestRoadHPMS"],
    ["PerPatSeriesCAFO"],
    ["PerPatSeriesLandfill"]
  ],
  yearStart = 2020,
  yearEnd = 2020
} [{
  year = 2020,
  skip = {
    csvTable = "run"
  }
}]