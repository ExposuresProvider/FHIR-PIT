library(data.table)

sink("/tmp/boxplot.txt")
print ("random")
random <- fread(paste0("/tmp/boxplot_random.csv"))
print(summary(random$maxpm))
for(i in 0:6) {
  print (paste0("d=",i))
  dfi <- fread(paste0("/tmp/boxplot_",i,".csv"))
  dfed <- dfi[dfi$visit_type == 'ED/INPATIENT,asthma-like']
  dfout <- dfi[dfi$visit_type == 'OUTPATIENT,any']
  print (paste0("ed/inpatient"))
  print(summary(dfed$maxpm))
  print (paste0("outpatient"))
  print(summary(dfout$maxpm))
  print ("t-test")
  print(t.test(random$maxpm, dfed$maxpm))
}
sink()