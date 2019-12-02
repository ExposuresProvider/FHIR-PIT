### run
in `spark` dir,
```
python src/main/python/runPreprocPipeline.py local <config file>
```

### config file format
```
- name: <name>
  dependsOn: 
  - <name>
  skip: <boolean>
  step:
    function: <functional name>
    arguments:
      <arg>: <value>
```





