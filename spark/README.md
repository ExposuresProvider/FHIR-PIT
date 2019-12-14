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


### generate config file

install dhall, dhall-to-json from

https://github.com/dhall-lang/dhall-haskell/releases

modify config/example.dhall

```
dhall-to-yaml --file config/example.dhall --output config/example.yaml
```

### run
```
python src/main/python/runPreprocPipeline.py <master url> <config file>
```

### config file format
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



