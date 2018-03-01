# spark data transformation tool #

## usage ##

```
spark-submit --master <spark master node url> --class datatrans.PreprocSeries <path to preproc_2.11-1.0.jar> (json|csv) <patient_dimension csv path> <visit_dimension csv path> <observation_fact csv path> <output path>
```

 * input format: csv with header

 * output format: json or csv sparse format

   * the json format: each line is a json object 

     * simple object `col` 

     * compound object `table`, `instance_num`, `modifier_cd`, `col`
