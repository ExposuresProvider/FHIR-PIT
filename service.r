# input file: input.json
# output file: output.json
# put model.RData and colnames.RData in the same directory
# put utils.r in the same directory

library(rpart)
library(data.table)
library(rjson)
library(partykit)

source(file="utils.r")

# https://stackoverflow.com/questions/29618490/get-decision-tree-rule-path-pattern-for-every-row-of-predicted-dataset-for-rpart
pathpred <- function(object, ...)
{
  ## coerce to "party" object if necessary
  if(!inherits(object, "party")) object <- as.party(object)
  
  ## get standard predictions (response/prob) and collect in data frame
  rval <- data.frame(response = predict(object, type = "response", ...))
  # rval$prob <- predict(object, type = "prob", ...)
  
  ## get rules for each node
  rls <- partykit:::.list.rules.party(object)
  
  ## get predicted node and select corresponding rule
  rval$rule <- rls[as.character(predict(object, type = "node", ...))]
  
  return(rval)
}

m <- readRDS("model.RData")
colname <- readRDS("colnames.RData")

json <- fromJSON(file = "input.json")
birth_date <- formatTime(json$date_of_birth)
sex_cd <- json$sex
race_cd <- json$race
all_visits <- json$visits

predict_post_ed <- function(visit) {
  start_date <- formatTime(visit$time)
  age <- binAge(as.double(difftime(start_date, birth_date, units="days")))
  inout_cd <- visit$visit_type
  pm25_7da <- visit$exposure[[1]]$value
  pre_ed <- binEdVisits(length(all_visits[all_visits$visit_type != "OUTPATIENT" && formatTime(all_visits$time) < start_date &&  formatTime(all_visits$time) >= start_date - 365]))
  newdata.base <- data.table(age, sex_cd, race_cd,start_date, inout_cd, pm25_7da, pre_ed)
  icd_code <- visit$icd_code
  newdata.icd <- as.data.frame(lapply(as.list(colname), FUN = function(icd_code2) {if (substring(icd_code2, 5) %in% icd_code) {TRUE} else {NA}}))
  colnames(newdata.icd) <- colname
  newdata <- cbind(newdata.base, newdata.icd)
  pp <- pathpred(m, newdata = newdata)
  print(start_date)
  resp <- data.table(endotype_id = paste0("E", toString(as.integer(pp$response) - 1)), endotype_description = pp$response, endotype_evidence = pp$rule, start_time=start_date)
}

predicts <- do.call("rbind", lapply(all_visits, FUN = predict_post_ed))
predicts.sorted <- predicts[order(predicts$start_time),]
predicts.sorted$end_time <- tail(c(predicts.sorted$start_time,predicts.sorted$start_time[-1]), n = nrow(predicts.sorted))
predicts.sorted$start_time <- unformatTime(predicts.sorted$start_time)
predicts.sorted$end_time <- unformatTime(predicts.sorted$end_time)
str<-toJSON(lapply(seq(nrow(predicts.sorted)), FUN = function(i) {
  row <- predicts.sorted[i]
  list(endotype_id = row$endotype_id, endotype_description = row$endotype_description, endotype_evidence = row$endotype_evidence, 
       periods= list(list(start_time=row$start_time, end_time = row$end_time)))
  }))
write(str, file="output.json")
