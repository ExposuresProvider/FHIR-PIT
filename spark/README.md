# Set up FHIR PIT and spark data transformation tool #
## Installation through Docker
Docker is an open platform which provides the ability to package and run an application in a loosely 
isolated environment called a container. Refer to 
[Docker documantation](https://docs.docker.com/get-started/overview/) for more details. 
- Install Docker. Follow instructions in [Docker installation documentation](https://docs.docker.com/engine/install/) 
for your operating system.
- Change directory to where you want to run FHIR PIT, e.g., `cd /home/user`
- Clone this repo.
```git clone --recursive https://github.com/ExposuresProvider/FHIR-PIT.git```
- Run FHIR PIT in a docker container with `-v` option to mount a volume from the host into the docker 
container where FHIR PIT will run. This will allow the output data to persist and be easily 
- accessible by users on the host.
```docker run -v /home/user/FHIR-PIT/data/output:/FHIR-PIT/data/output -it renci/fhir-pit:1.0```
Make sure `/home/user/FHIR-PIT/data/output` directory corresponds to the directory you want to 
hold FHIR-PIT run output data in your operating system host environment. 
FHIR PIT should run inside the docker container with status and progress information displayed in 
the console while it is running. After it is done running, you should be able to access the output
from FHIR PIT in the directory `/home/user/FHIR-PIT/data/output`.
- You can run the command `docker rm -fv $(docker ps -a -q)` to clean up the FHIR PIT container after 
it is done running.

## Installation in your native operating system environment without using Docker
- install the interactive build tool sbt. Refer to [Scala sbt page](https://www.scala-sbt.org/) for details.
- install apache spark with hadoop. Refer to [Apache Spark page ](https://spark.apache.org/) for details. 
Make sure you install pyspark version 2.4.5 since the newer pyspark version may not work with older Java versions 
such as 1.8. We recommend to create a python 3 virtual environment and install pyspark version 2.4.5 in the virtual 
environment by following the steps below.
```
python -m venv fhir-pit
source fhir-pit/bin/activate
pip install pyspark==2.4.5
```
- install dhall by following the sample steps below.
  ```
  mkdir dhall_1.42.0 
  cd dhall_1.42.0
  wget https://github.com/dhall-lang/dhall-haskell/releases/download/1.42.0/dhall-yaml-1.2.12-x86_64-linux.tar.bz2
  tar -xvf dhall-yaml-1.2.12-x86_64-linux.tar.bz2
  export PATH=<absolute_dir>/dhall_1.42.0/bin:$PATH
  ```
  Refer to [dhall page](https://github.com/dhall-lang/dhall-haskell/releases) for details.

## Clone this repo
```
git clone --recurse-submodules https://github.com/ExposuresProvider/FHIR-PIT.git
```

## build sbt jar

```
cd FHIR-PIT/spark
SBT_OPTS=-Xmx64G sbt 'set test in assembly := {}' assembly
```

## test

```
sbt test
```
## Config FHIR-PIT via dhall
- Modify `FHIR-PIT/spark/config/example_demo.dhall` as needed. To run FHIR PIT with sample data for demo purposes: 

    - Update variable `basedir` with the location of the FHIR-PIT repo. 
    - Update variable `python_exec` with the location of the python executable. FHIR PIT is tested to run successfully 
  in a virtual environment created in python 3.6.9 and above, so it is advised to create a python virtual environment 
  and point the python_exec to the python executable location in your virtual environment. 

- Generate yaml config file.
    ```
    dhall-to-yaml-ng --file config/example_demo.dhall --output config/example_demo.yaml
    ```

## run
Activate your python virtual environment (python 3.6.9 and above), cd into FHIR-PIT/spark folder and execute:
```
python src/main/python/runPreprocPipeline.py <master url> <yaml config file>
```
Where `<master_url>` is the [supported master URL](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
for the spark cluster and `<config file>` is the desired dhall or YAML config file for the run. 
If FHIR-PIT is run via Spark with one worker thread, `local` can be passed as the <master url> to run FHIR-PIT 
locally with no parallelism. 

For example: `python src/main/python/runPreprocPipeline.py local ./config/example_demo.yaml`

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


