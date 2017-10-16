# input file: input.json
# output file: output.json
# put model.RData and colnames.RData in the same directory
# put utils.r in the same directory

library(rpart)
library(data.table)
library(rjson)
library(partykit)

str <- tryCatch({
  source(file="utils.r")
  
  args <- commandArgs(trailingOnly=TRUE)
  if(length(args) == 0) {
    input <- "input.json"
    output <- "output.json"
  } else {
    input <- args[1]
    output <- args[2]
  }
  
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
  
  json <- fromJSON(file = input)
  birth_date <- formatDate(json$date_of_birth)
  sex_cd <- json$sex
  race_cd <- json$race
  all_visits <- json$visits
  
  get_exposure_by_type <- function(type, default, exposures) {
    exps <- Filter(function(x) x$exposure_type==type, exposures)
    if(length(exps) == 0) default else as.numeric(exps[[1]]$value)
  }
  
  predict_post_ed <- function(visit) {
    start_date <- formatTime(visit$time)
    age <- binAge(as.double(difftime(start_date, birth_date, units="days")))
    inout_cd <- visit$visit_type
    despm_7da <- get_exposure_by_type("pm25", 2.5, visit$exposure)
    deso_7da <- get_exposure_by_type("ozone", 2.5, visit$exposure)
    pre_ed <- binEdVisits(length(all_visits[all_visits$visit_type != "OUTPATIENT" && formatTime(all_visits$time) < start_date &&  formatTime(all_visits$time) >= start_date - 365]))
    newdata.base <- data.table(age, sex_cd, race_cd,start_date, inout_cd, despm_7da, pre_ed, deso_7da)
    icd_codes <- visit$icd_codes
    newdata.icd <- as.data.frame(lapply(as.list(colname), FUN = function(icd_code2) {icd_code2 %in% icd_codes}))
    colnames(newdata.icd) <- colname
    newdata <- cbind(newdata.base, newdata.icd)
    print(newdata)
    pp <- pathpred(m, newdata = newdata)
    resp <- data.table(endotype_id = paste0("E", toString(as.integer(pp$response) - 1)), endotype_description = pp$response, endotype_evidence = pp$rule, start_time=start_date)
  }
  
  predicts <- do.call("rbind", lapply(all_visits, FUN = predict_post_ed))
  predicts.sorted <- predicts[order(predicts$start_time),]
  predicts.sorted$end_time <- tail(c(predicts.sorted$start_time,predicts.sorted$start_time[-1]), n = nrow(predicts.sorted))
  predicts.sorted$start_time <- unformatTime(predicts.sorted$start_time)
  predicts.sorted$end_time <- unformatTime(predicts.sorted$end_time)
  toJSON(lapply(seq(nrow(predicts.sorted)), FUN = function(i) {
    row <- predicts.sorted[i]
    list(endotype_id = row$endotype_id, endotype_description = row$endotype_description, endotype_evidence = row$endotype_evidence, 
         periods= list(list(start_time=row$start_time, end_time = row$end_time)))
  }))
}, error = function(e) {
  print(paste0("error: ", toString(e)))
  toJSON(list(error = toString(e)))
})

write(str, file=output)
