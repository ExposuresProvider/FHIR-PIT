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

## Setup dhall

install dhall, dhall-to-json from

https://github.com/dhall-lang/dhall-haskell/releases

## Modify `config/example_demo.dhall`

Update variable `basedir` with the location of the FHIR-PIT repo.

Update variable `python_exec` with the location of the python executable.

## Generate config file.

```
dhall-to-yaml --file config/example_demo.dhall --output config/example_demo.yaml
```

## run

cd into FHIR-PIT/spark folder and execute:
```
python src/main/python/runPreprocPipeline.py <master url> <config file>
```
Where `<master_url>` is the [master url](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) for the spark cluster and `<config file>` is the desired YAML config file for the run.

For example: `python src/main/python/runPreprocPipeline.py "local" "./config/example_demo.yaml"`

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

`split.py`: split the dataset by `patient_num` this is the same as `PreprocSplit.scala` but runs on single machine

`envDataAggregate.py`: aggregate data by year this is the same as `PreprocEnvDataAggregate.scala` but runs on single machine

#### dataset 2

`convert_environmental_date_format.py`: convert environmental file date format from mm-dd-yy to yy/mm/dd

`merge_env.py`: merge env csvs

### medication request

`rxcui_dict.py`: generate icees features rxnorm json from a xslx file

### icees
`perPatSeriesCSVTable.py`: generate icees table this is the same as `PreprocPerPatSeriesCSVTable.scala` but runs on single machine

`preprocPatient.py`: bin patient values

`preprocVisit.py`: bin visit values

`preprocBinning.py`: preprocess binning

### fhir

`merge_fhir.py`: merge multiple fhir datasets into one

### generic

`concat_dfs.py`: concatenate a list of csv

`create_shapefile.py`: create a shapefile for testing the `EnvData` step

`merge_dfs.py`: merge a list of csv

`stats.py`: generate statistics of a list of csvs

`split.py`: split by index

`submit.py`: submit


