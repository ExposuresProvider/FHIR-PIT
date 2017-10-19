library(data.table)

df <- fread("/tmp/boxplot.csv")

boxplot(despm_7da~visit_type,data=df)
boxplot(deso_7da~visit_type,data=df)

