## cooccurrence ##

### compile ###

```
cd cpp
```
clang:
```
clang++ -std=c++14 -I../../sparse/cpp/ ../../sparse/cpp/import_df.cpp cooccurrence.cpp utils.cpp feature.cpp -o cooccurrence
```
g++:
```
g++ -std=c++0x -I../../sparse/cpp/ ../../sparse/cpp/import_df.cpp cooccurrence.cpp utils.cpp feature.cpp -o cooccurrence
```

### usage ###
```
./aggregator <number of criteria> <criteria> <number of bins> <bins> <output> <input> <filemeta> <colmeta>
```

## aggregator ##

### compile ###

```
cd cpp
```
clang:
```
clang++ -std=c++14 -I../../sparse/cpp/ ../../sparse/cpp/import_df.cpp aggregate.cpp utils.cpp feature.cpp -o aggregator
```
g++:
```
g++ -std=c++0x -I../../sparse/cpp/ ../../sparse/cpp/import_df.cpp aggregate.cpp utils.cpp feature.cpp -o aggregator
```

### usage ###
```
./aggregator <number of criteria> <criteria> <inout_cd_filters> <rxnorm_gene_map> <number of years> <years> <output> <input> <filemeta> <colmeta>
```

 * `<number of criteria>` number of criteria
 * `<criteria>` currently support three argment format `<prop> <op> <val>` where 
   - `<prop>` is `age`
   - `<op>` is `<`, `>`, `==`, `!=`, `<=`, or `>=` and 
   - `<val>` is an integer
 * `<inout_cd_filter>` is a filename. The file contains the keys that indicates that we should count inout_cd.
 * `<rxnorm_gene_map>` is a filename. The file contains rxnorm to gene map, one per line, separator ` `.
 * `<number of years>` is the number of years.
 * `<years>` is a list of years to aggregate.
 * `<output>` is a filename. The file is where output csv is written, seperator `!`.
 * `<input>` is the input filename.
 * `<filemeta>` is the input metadata filenane.
 * `<colmeta>` is a list of `<col name> <meta>`.

For example,
```
./aggregator 0 inoutcdfilters medgenemap 3 2014 2015 2016 aggregation endotype_filter_trunc.csv endotype_filter_trunc_meta.csv icd icd_filter_trunc_meta.csv loinc loinc_filter_trunc_meta.csv mdctn mdctn_rxnorm_filter_trunc_meta.csv gene mdctn_gene_filter_trunc_meta.csv vital vital_filter_trunc_meta.csv
```
