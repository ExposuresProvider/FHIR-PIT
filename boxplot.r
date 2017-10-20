library(data.table)

random <- fread(paste0("/tmp/boxplot_random.csv"))
for(i in 0:6) {
  dfi <- fread(paste0("/tmp/boxplot_",i,".csv"))
  df <- rbind(dfi, random)
  png(paste0("/tmp/boxplot_", i, ".png"), width=800, height = 1600)
  boxplot(maxpm~visit_type,data=df, xlab="visit_type", ylab="maxpm", main=paste0("d=", i))
  dev.off()
  png(paste0("/tmp/boxplot_log10_", i, ".png"), width=800, height = 600)
  df$maxpm <- log10(df$maxpm)
  boxplot(maxpm~visit_type,data=df, xlab="visit_type", ylab="log10(maxpm)", main=paste0("d=", i))
  dev.off()
}

