library(rpart)
library(rpart.plot)
library(data.table)
library(reshape)
library(dplyr)

df.pat <- fread("/tmp/endotype3.csv")
df.icd <- fread("/tmp/icd3.csv")
df.loinc <- fread("/tmp/loinc3.csv")
df.mdctn <- fread("/tmp/mdctn3.csv")

df.loinc.wide <- reshape(df.loinc, idvar = "encounter_num", timevar = "loinc_concept", direction = "wide")
df.mdctn.wide <- reshape(df.loinc, idvar = "encounter_num", timevar = c("mdctn_cd", "mdctn_modifier"), direction = "wide")
df.icd$icd <- TRUE
df.icd.wide <- reshape(df.icd, idvar = "encounter_num", timevar = "icd_code", direction = "wide")

df.pat.2 <- df.pat[,c("encounter_num", "inout_cd", "pre_ed", "post_ed", "sex_cd", "race_cd", "pm25_7da", "age")]
df.pat.2$age <- df.pat.2$age/365.25
df.pat.2$race_cd <- as.factor(df.pat.2$race_cd)
df.pat.2$sex_cd <- as.factor(df.pat.2$sex_cd)
df.pat.2$inout_cd <- as.factor(df.pat.2$inout_cd)

df <- merge(merge(merge(df.pat.2, df.mdctn.wide, by = "encounter_num"), df.icd.wide, by = "encounter_num"), df.loinc.wide, by = "encounter_num")

df.colnames <- colnames(df)
loinc.colnames <- paste0("`", df7.colnames[substr(df7.colnames,0,17) == "loinc_nval.LOINC:" | substr(df7.colnames,0,4) == "icd."], "`")
loinc.form <- paste(loinc.colnames, collapse = " + ")
formstr <- paste("post_ed ~ inout_cd + pre_ed + sex_cd + race_cd + age + pm25_7da", 
              loinc.form, sep = " + ")
form <- as.formula(formstr)
df8 <- df7 %>% mutate_if(is.numeric, scale)

m <- rpart(form, data = df8, 
           control=rpart.control(cp=0.01))
print(m)
rpart.plot(m)
printcp(m)
