daysToYears <- function(age) age/365.25
binAge <- function(age) cut(daysToYears(age), breaks = c(0,3,18,45,65,Inf),right = FALSE)
binEdVisits <- function(ed_visits) cut(ed_visits, breaks=c(0,0.5,1.5,2.5,Inf),right = FALSE)
formatTime <- function(str) strptime(str, format="%Y-%m-%d %H:%M:%S")
formatDate <- function(str) strptime(str, format="%Y-%m-%d")
unformatTime <- function(time) strftime(time)