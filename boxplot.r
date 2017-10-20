library(data.table)

for(i in 0:6) {
  df <- fread(paste0("/tmp/boxplot_",i,".csv"))
  png(paste0("/tmp/boxplot_", i, ".png"), width=800, height = 600)
  boxplot(despm~visit_type,data=df, xlab="visit_type", ylab="despm", main=paste0("d=", i))
  dev.off()
}

