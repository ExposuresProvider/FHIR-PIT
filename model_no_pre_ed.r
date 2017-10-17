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

df.pat.2 <- df.pat[,c("encounter_num", "patient_num", "inout_cd", "pre_ed", "post_ed", "sex_cd", "race_cd", "despm_7da", "deso_7da", "age")]
df.pat.2$age <- binAge(df.pat.2$age)
df.pat.2$race_cd <- as.factor(df.pat.2$race_cd)
df.pat.2$pre_ed <- binEdVisits(df.pat.2$pre_ed)
df.pat.2$post_ed <- binEdVisits(df.pat.2$post_ed)
df.pat.2$sex_cd <- as.factor(df.pat.2$sex_cd)
df.pat.2$inout_cd <- as.factor(df.pat.2$inout_cd)

#df <- merge(merge(df.pat.2, df.icd.wide, by = "encounter_num"), df.loinc.wide, by = "encounter_num")
df <- merge(df.pat.2, df.icd.wide, by = c("encounter_num", "patient_num"))

#loinc.colnames <- paste0("`", df.colnames[substr(df.colnames,0,17) == "loinc_nval.LOINC:" | substr(df.colnames,0,4) == "icd."], "`")
icd.wide.colnames.quoted <- paste0("`", icd.wide.colnames, "`")
icd.form <- paste(icd.wide.colnames.quoted, collapse = " + ")
formstr <- paste("post_ed ~ inout_cd + sex_cd + race_cd + age + despm_7da + deso_7da", 
              icd.form, sep = " + ")
form <- as.formula(formstr)
#df.scaled <- df %>% mutate_if(is.numeric, scale)

#m <- rpart(form, data = df.scaled, 
#           control=rpart.control(cp=0.003))
m <- rpart(form, data = df, 
           control=rpart.control(cp=0.001))
print(m)
rpart.plot(m)
#printcp(m)
saveRDS(as.party(m), file="model.RData")
saveRDS(icd.wide.colnames, file="colnames.RData")

