```
singularity run --app preprocFIHR -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/FIHR Encounter,Condition,MedicationRequest /data/json

singularity run --app preprocCMAQ -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/cmaq/cmaq2010.csv /data/cmaq2010

singularity run --app preprocCMAQ2 -B .:/data ../datatrans/spark/sing/spark/spark2.img /data/cmaq /data/cmaq2011

singularity run --app preprocDailyEnvData -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data 2010

singularity run --app preprocDailyEnvData -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data 2011

singularity run --app preprocPerPatSeriesEnvData -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/patient_ids /data/json /data 2010-1-1 2012-1-1 /data/env

singularity run --app preprocPerPatSeriesNearestRoad -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/patient_ids /data/json /data/tl_2015_allstates_prisecroads_lcc.shp /data/nearestroad

singularity run --app preprocPerPatSeriesACS -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/patient_ids /data/json /data/tl_2016_37_bg_lcc.shp /data/ACS_NC_2016_with_column_headers.csv /data/acs

singularity run --app preprocCombineData -B .:/data ../datatrans/spark/sing/spark/spark2.img rockfish /data/patient_ids /data/json /data env,nearestroad,acs /data/icees
```

