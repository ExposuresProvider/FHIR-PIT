# spark data transformation tool #

## build sbt jar

```
sbt assembly
```

## build docker container

```
docker build spark -t fhir-pit:0.1.0
```

## test

```
sbt test
```


## generate config file

install dhall, dhall-to-json from

https://github.com/dhall-lang/dhall-haskell/releases

modify `config/example2.dhall`

```
dhall-to-yaml --file config/example2.dhall --output config/example2.yaml
```

## run
```
python src/main/python/runPreprocPipeline.py <master url> <config file>
```

## config file format
```
- name: <name>
  dependsOn: 
  - <name>
  skip: <skip>
  step:
    function: <functional name>
    arguments:
      <arg>: <value>
```

## troubleshooting

### run out of memory

set `SBT_OPTS`

### run out disk space for temp

set `SPARK_LOCAL_DIRS`

## utilities:

### env

#### dataset 1

`preproc_cmaq_2011.py`: preprocess 2011 cmaq data into `runPreprocDailyEnvData.py` input format

`runPreprocCMAQ.py`: preprocess 2010 cmaq data into `runPreprocDailyEnvData.py` input format

`runPreprocDailyEnvData.py`: daily to yearly stats

#### dataset 2

`convert_environmental_date_format.py`: convert environmental file date format from mm-dd-yy to yy/mm/dd

`merge_env.py`: merge env csvs

### medication request

`rxcui_dict.py`: generate icees features rxnorm json from a xslx file

### icees

`preprocPatient.py`: bin patient values

`preprocVisit.py`: bin visit values

### fhir

`merge_fhir.py`: merge multiple fhir datasets into one

### generic

`concat_dfs.py`: concatenate a list of csv

`create_shapefile.py`: create a shapefile for testing the `EnvData` step

`merge_dfs.py`: merge a list of csv

`stats.py`: generate statistics of a list of csvs



