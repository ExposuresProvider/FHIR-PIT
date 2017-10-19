library(data.table)

df <- fread("/tmp/boxplot.csv")

boxplot(despm_7da~visit_type,data=df, xlab="visit_type", ylab="despm_7da")

dev.copy(png, "pm25_7da_visit_type.png", width=800, height = 600)
dev.off()

