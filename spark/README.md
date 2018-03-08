# spark data transformation tool #

## usage ##

### chunk input files

```
/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://a-HP-Z820-Workstation:7077 --class datatrans.PreprocPerPatSeries target/scala-2.11/preproc_2.11-1.0.jar <patient dimension> <visit dimension> <observation fact> <output path> 
```

### convert to json

```
/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://a-HP-Z820-Workstation:7077 --class datatrans.PreprocPerPatSeries target/scala-2.11/preproc_2.11-1.0.jar <patient dimension> <chunks path> <output path prefix>
```

### per encounter join
```
spark-submit --master <spark master node url> --class datatrans.PreprocSeries <path to preproc_2.11-1.0.jar> (json|csv) <patient_dimension csv path> <visit_dimension csv path> <observation_fact csv path> <output path>
```

 * input format: csv with header

 * output format: json or csv sparse format

   * the json format: each line is a json object 

     * simple object `col` 

     * compound object `table`, `instance_num`, `modifier_cd`, `col`


