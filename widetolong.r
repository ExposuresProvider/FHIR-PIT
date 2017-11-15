library(rpart)
library(rpart.plot)
library(data.table)
library(reshape)
library(dplyr)
library(partykit)
source(file="utils.r")

df.pat <- fread("/tmp/endotype3.csv")
df.icd <- fread("/tmp/icd3.csv")
# df.loinc <- fread("/tmp/loinc3.csv")
# df.mdctn <- fread("/tmp/mdctn3.csv")

# df.loinc.wide <- reshape(df.loinc, idvar = "encounter_num", timevar = "loinc_concept", direction = "wide")
# df.mdctn.wide <- reshape(df.loinc, idvar = "encounter_num", timevar = c("mdctn_cd", "mdctn_modifier"), direction = "wide")
df.icd$icd <- TRUE
df.icd.wide <- cast(df.icd, encounter_num + patient_num ~ icd_code, fill = FALSE)
df.icd.wide.colnames <- colnames(df.icd.wide)
icd.wide.colnames <- df.icd.wide.colnames[substr(df.icd.wide.colnames,1,3) == "ICD"]
df.icd.wide[icd.wide.colnames] <- lapply(df.icd.wide[icd.wide.colnames], as.factor)
df <- merge(df.pat, df.icd.wide, by = c("encounter_num", "patient_num"))

fwrite(df, "/tmp/features.csv")
