# spark data transformation tool #

## usage ##

### chunk input files

```
/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://a-HP-Z820-Workstation:7077 --class datatrans.PreprocPerPatSeries target/scala-2.11/preproc_2.11-1.0.jar <patient dimension> <visit dimension> <observation fact> <output path> 
```

### convert to json

```
/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://a-HP-Z820-Workstation:7077 --jars ~/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar --class datatrans.PreprocPerPatSeries target/scala-2.11/preproc_2.11-1.0.jar --patient_dimension=/mnt/d/patient_dimension.csv --input_directory=/mnt/d/patient_series --output_prefix=/mnt/d/json/
```

### convert to vector

```
/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://a-HP-Z820-Workstation:7077 --jars ~/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar --class datatrans.PreprocPerPatSeries target/scala-2.11/preproc_2.11-1.0.jar --patient_dimension=/mnt/d/patient_dimension.csv --input_directory=/mnt/d/patient_series --output_prefix=/mnt/d/json/
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


