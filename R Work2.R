class <- c("Pump_Status_1" = "character", "Pump_Status_2" = "character", "Pump_Status_3" = "character",
           "Pump_Status_4" = "character", "Pump_Status_5" = "character")

change_PumpType <- function(dat){
  dat$Pump_Status_1 <- as.character(dat$Pump_Status_1)
  dat$Pump_Status_2 <- as.character(dat$Pump_Status_2)
  dat$Pump_Status_3 <- as.character(dat$Pump_Status_3)
  dat$Pump_Status_4 <- as.character(dat$Pump_Status_4)
  dat$Pump_Status_5 <- as.character(dat$Pump_Status_5)
  return(dat)
}



rxDataStep(Line2, outFile = "Line21.xdf", transformFunc = change_PumpType)
Line21 <- RxXdfData("Line21.xdf")
rxGetInfo(Line21, getVarInfo = T)

pivot <- function(dat){
  datp <- spread(dat, TagType, Value)
  datp <- datp[c(1,2,3,4,5,8,6,16,7,9,10,11,12,13,14,15)]
  return(datp)
}

rxDataStep(Line2up, outFile = "Line2a.xdf", overwrite = T, transformFunc = pivot,
           transformPackages = "tidyr", transformVars = c("TagType", "Value"))

change_PumpStatus <- function(dat){
  
  dat$Pump_Status_N1 <- ifelse(dat$Pump_Status_1 == "ON", 1,
                               ifelse(dat$Pump_Status_1 == "IN SEQUENCE ON", 1, 0))
  dat$Pump_Status_N2 <- ifelse(dat$Pump_Status_2 == "ON", 1,
                               ifelse(dat$Pump_Status_2 == "IN SEQUENCE ON", 1, 0))
  dat$Pump_Status_N3 <- ifelse(dat$Pump_Status_3 == "ON", 1,
                               ifelse(dat$Pump_Status_3 == "IN SEQUENCE ON", 1, 0))
  dat$Pump_Status_N4 <- ifelse(dat$Pump_Status_4 == "ON", 1,
                               ifelse(dat$Pump_Status_4 == "IN SEQUENCE ON", 1, 0))
  dat$Pump_Status_N5 <- ifelse(dat$Pump_Status_5 == "ON", 1,
                               ifelse(dat$Pump_Status_5 == "IN SEQUENCE ON", 1, 0))
  
  dat[c(17,18,19,20,21)][is.na(dat[c(17,18,19,20,21)])] <- 0
  dat$NumPumpsOn <- dat$Pump_Status_N1 + dat$Pump_Status_N2 + dat$Pump_Status_N3 + dat$Pump_Status_N4 + dat$Pump_Status_N5
  
  dat$TotalPumps <- ifelse(is.na(dat$Pump_Status_1) == TRUE, 0,
                           ifelse(is.na(dat$Pump_Status_2) == TRUE, 1,
                                  ifelse(is.na(dat$Pump_Status_3) == TRUE, 2,
                                         ifelse(is.na(dat$Pump_Status_4) == TRUE, 3,
                                                ifelse(is.na(dat$Pump_Status_5) == TRUE, 4, 5)))))
  
  dat <- dat[c(1:16,22,23)]
  
  return(dat)
}

sdf1 <- change_PumpStatus(sdf)
head(sdf1)

rxDataStep(Line21, outFile = "Line22.xdf", overwrite = T, transformFunc = change_PumpStatus)
Line22 <- RxXdfData("Line22.xdf")
rxGetInfo(Line22,getVarInfo = T)
head(Line22)

PV <- function(a,b,c,d,e){
  
  x <- 0
  y <- 0
  on <- c("ON", "IN SEQUENCE ON")
  
  if (a %in% on){x <- x + 1}
  if (b %in% on){x <- x + 1}
  if (c %in% on){x <- x + 1}
  if (d %in% on){x <- x + 1}
  if (e %in% on){x <- x + 1}
  
  if (is.na(a) == TRUE) {y <- 0
  } else if (is.na(b) == TRUE) {y <- 1
  } else if (is.na(c) == TRUE) {y <- 2
  } else if (is.na(d) == TRUE) {y <- 3
  } else if (is.na(e) == TRUE) {y <- 4
  } else {y <- 5}
  
  NumPumpsOn <- c(NumPumpsOn,x)
  TotalPumps <- c(TotalPumps,y)
  return(list(NumPumpsOn,TotalPumps))
}

head(mapply(PV, sdf$Pump_Status_1, sdf$Pump_Status_2, sdf$Pump_Status_3, sdf$Pump_Status_4, sdf$Pump_Status_5))

mutate()

mut_PV <- function(a,b,c,d,e){
  on <- c("ON", "IN SEQUENCE ON")
  x <- 0
  
  if (a %in% on){x <- x + 1}
  if (b %in% on){x <- x + 1}
  if (c %in% on){x <- x + 1}
  if (d %in% on){x <- x + 1}
  if (e %in% on){x <- x + 1}
  
  return(x)
}

sdf %>% mutate(NumPumpsOn = mut_PV(Pump_Status_1,Pump_Status_2,Pump_Status_3,Pump_Status_4,Pump_Status_5)) %>% head


if (is.na(x$Pump_Status_1) == TRUE) {y <- 0}
else if (is.na(x$Pump_Status_1) == TRUE) {y <- 1}
else if (is.na(x$Pump_Status_1) == TRUE) {y <- 2}
else if (is.na(x$Pump_Status_1) == TRUE) {y <- 3}
else if (is.na(x$Pump_Status_1) == TRUE) {y <- 4}
else {y <- 5}


change_PumpStatus <- function(dat){
  
  dat$Pump_Status_N1 <- ifelse(dat$Pump_Status_1 == "ON", 1,
                               ifelse(dat$Pump_Status_1 == "IN SEQUENCE ON", 1,
                                      ifelse(is.na(dat$Pump_Status_1) == TRUE, 0, 0)))
  dat$Pump_Status_N2 <- ifelse(dat$Pump_Status_2 == "ON", 1,
                               ifelse(dat$Pump_Status_2 == "IN SEQUENCE ON", 1,
                                      ifelse(is.na(dat$Pump_Status_2) == TRUE, 0, 0)))
  dat$Pump_Status_N3 <- ifelse(dat$Pump_Status_3 == "ON", 1,
                               ifelse(dat$Pump_Status_3 == "IN SEQUENCE ON", 1,
                                      ifelse(is.na(dat$Pump_Status_3) == TRUE, 0, 0)))
  dat$Pump_Status_N4 <- ifelse(dat$Pump_Status_4 == "ON", 1,
                               ifelse(dat$Pump_Status_4 == "IN SEQUENCE ON", 1,
                                      ifelse(is.na(dat$Pump_Status_4) == TRUE, 0, 0)))
  dat$Pump_Status_N5 <- ifelse(dat$Pump_Status_5 == "ON", 1,
                               ifelse(dat$Pump_Status_5 == "IN SEQUENCE ON", 1,
                                      ifelse(is.na(dat$Pump_Status_5) == TRUE, 0, 0)))
  
  dat$NumPumpsOn <- dat$Pump_Status_N1 + dat$Pump_Status_N2 + dat$Pump_Status_N3 + dat$Pump_Status_N4 + dat$Pump_Status_N5
  
  dat$TotalPumps <- ifelse(is.na(dat$Pump_Status_1) == TRUE, 0,
                           ifelse(is.na(dat$Pump_Status_2) == TRUE, 1,
                                  ifelse(is.na(dat$Pump_Status_3) == TRUE, 2,
                                         ifelse(is.na(dat$Pump_Status_4) == TRUE, 3,
                                                ifelse(is.na(dat$Pump_Status_5) == TRUE, 4, 5)))))
  
  dat <- dat[c(1:16,22,23)]
  
  return(dat)
}


NAreplace <- function(dataList) {
  replaceFun <- function(x) {
    x[is.na(x)] <- 0
    return(x)
  }
  dataList <- lapply(dataList, replaceFun)
  return(dataList)
}

Line22 <- rxDataStep(Line21, outFile = "Line22.xdf", overwrite = T, transformFunc = NAreplace)







t1 <- as.POSIXct("2010-07-18 07:48:38", tz = "MST")
t2 <- as.POSIXct("2015-04-21 20:46:34", tz = "MST")
d <- as.numeric(difftime(t2,t1,units=c("mins")))

t <- seq(t1,t2,by="min")

l <- "5-BC-MV"
unlist(strsplit(l,split="-"))


df$Diff <- as.numeric(difftime(df$END_DATE,df$START_DATE,units=c("mins")))
df1 <- ddply(df, .(id, ACTIVITY_TYPE_CODE, COMMODITY_ID, DESTINATION_FCLTY_NAME, DESTINATION_SITE_CALL_NAME, OBSERVED_VOL, OBSERVED_IND),
              transform, Min=seq(START_DATE, END_DATE, by="min"))


blowup_time <- function(dat){
  
  dat1 <- data.frame("ACTIVITY_TYPE_CODE" = character(0),
                     "COMMODITY_ID" = character(0),
                     "DESTINATION_FCLTY_NAME" = character(0),
                     "DESTINATION_SITE_CALL_NAME" = character(0),
                     "OBSERVED_VOL" = numeric(0),
                     "OBSERVED_IND" = character(0),
                     "TIMESTAMP" = character(0))
  
  for (i in 1:nrow(dat)){
    act <- as.character(dat[i,'ACTIVITY_TYPE_CODE'])
    com <- as.character(dat[i,'COMMODITY_ID'])
    seg <- as.character(dat[i,'DESTINATION_FCLTY_NAME'])
    site <- as.character(dat[i,'DESTINATION_SITE_CALL_NAME'])
    vol <- dat[i,'OBSERVED_VOL']
    ind <- as.character(dat[i,'OBSERVED_IND'])
    st <- dat[i,'START_DATE']
    end <- dat[i, 'END_DATE']
    
    d <- as.numeric(difftime(end, st, units=c("mins"))) + 1
    timestamp <- seq(st, end, by = "min")
    timestamp <- as.character(timestamp)
    
    
    dat2 <- data.frame("ACTIVITY_TYPE_CODE" = rep(act, d),
                       "COMMODITY_ID" = rep(com ,d),
                       "DESTINATION_FCLTY_NAME" = rep(seg, d),
                       "DESTINATION_SITE_CALL_NAME" = rep(site, d),
                       "OBSERVED_VOL" = rep(vol, d),
                       "OBSERVED_IND" = rep(ind, d),
                       "TIMESTAMP" = timestamp
                       )
    
    dat1 <- rbind(dat1,dat2)
  }
  
  return(dat1)
}

df1 <- blowup_time(df)


dat1 <- data.frame("ACTIVITY_TYPE_CODE" = character(0),
                   "COMMODITY_ID" = character(0),
                   "DESTINATION_FCLTY_NAME" = character(0),
                   "DESTINATION_SITE_CALL_NAME" = character(0),
                   "OBSERVED_VOL" = numeric(0),
                   "OBSERVED_IND" = character(0),
                   "TIMESTAMP" = character(0))


dat1 <- data.frame("ACTIVITY_TYPE_CODE" = character(0),
                   "COMMODITY_ID" = character(0),
                   "DESTINATION_FCLTY_NAME" = character(0),
                   "DESTINATION_SITE_CALL_NAME" = character(0),
                   "OBSERVED_VOL" = numeric(0),
                   "OBSERVED_IND" = character(0),
                   "TIMESTAMP" = character(0))

act <- as.character(df[1,'ACTIVITY_TYPE_CODE'])
com <- as.character(df[1,'COMMODITY_ID'])
seg <- as.character(df[1,'DESTINATION_FCLTY_NAME'])
site <- as.character(df[1,'DESTINATION_SITE_CALL_NAME'])
vol <- df[1,'OBSERVED_VOL']
ind <- as.character(df[1,'OBSERVED_IND'])
st <- df[1,'START_DATE']
end <- df[1, 'END_DATE']
  
d <- as.numeric(difftime(end, st, units=c("mins"))) + 1
timestamp <- seq(st, end, by = "min")
timestamp <- as.character(timestamp)
  
  
dat2 <- data.frame("ACTIVITY_TYPE_CODE" = rep(act, d),
                    "COMMODITY_ID" = rep(com ,d),
                    "DESTINATION_FCLTY_NAME" = rep(seg, d),
                    "DESTINATION_SITE_CALL_NAME" = rep(site, d),
                    "OBSERVED_VOL" = rep(vol, d),
                    "OBSERVED_IND" = rep(ind, d),
                    "TIMESTAMP" = timestamp
)
  
dat1 <- rbind(dat1,dat2)


library(data.table)
dt <- data.table(df)
dt <- dt[,seq(START_DATE,END_DATE,by="min"),
         by=list("ACTIVITY_TYPE_CODE","COMMODITY_ID","DESTINATION_FCLTY_NAME","DESTINATION_SITE_CALL_NAME", "OBSERVED_VOL", "OBSERVED_IND")]
setnames(dt,"V1","TIMESTAMP")
head(dt)


df <- filter()
df$id <- seq(1,nrow(df))

library(data.table)
dt <- data.table(df)
dt <- dt[,seq(START_DATE,END_DATE,by="min"),
         by=list(id,ACTIVITY_TYPE_CODE,COMMODITY_ID,DESTINATION_FCLTY_NAME,DESTINATION_SITE_CALL_NAME,OBSERVED_VOL,OBSERVED_IND)]
setnames(dt,"V1","TIMESTAMP")
head(dt)

flow0_st <- c()
i <- 1
Line2_st <- rxDataStep(Line2, outFile = "Line2_st.xdf", overwrite = T,
                       rowSelection = (pump_station == st[t]), transformObjects = list(t = i, st = station))
n <- nrow(Line2_st)
flow0_st <- c(flow0_st,n)
file.remove("Line2_st.xdf")

rxCrossTabs(~pump_station:pump_total:line_segment,Line2,removeZeroCounts = T, marginals = T)


# Create Folds

create_Folds <- function(df=Line2,st="station",n=2){

  p <- 1/n
  str <- st
  stu <- unique(df[str])
  nst <- nrow(stu)
  folds <- list()
  df_folds <- df[0,]
  df_folds <- cbind(df_folds, fold = numeric(0))
  
  for (i in 1:nst){
    
    dfx <- subset(df, df[,str]==stu[i,1])
    len <- floor(p*nrow(dfx))
    rest <- nrow(dfx) - n*len
    
    fold_st <- c()
    for (j in 1:n){
      fold_st <- c(fold_st, rep(j,len))
    }
    fold_st <- c(fold_st, rep(n+1,rest))
    fold_st <- sample(fold_st)
    dfx$fold <- fold_st
    df_folds <- rbind(df_folds,dfx)
    
  }
  
  for (i in 1:n){
    dfx <- subset(df_folds, fold == i)
    folds[[i]] <- dfx
  }
  
  return(folds)
  
}

# Testing

x <- rnorm(100)
y <- rnorm(100)
st <- sample(1:5,100, replace = T)
st <- ifelse(st == 1, "a", ifelse(st == 2, "b", ifelse(st == 3, "c", ifelse(st == 4, "d", "e"))))
df <- data.frame(x,y,st)

f <- create_Folds(df,'st',n=10)


Line2 <- RxXdfData("Line2.xdf")
rxDataStep(Line2, outFile = "Line2_Pred_Test.xdf",
           varsToDrop = c("kw", "pump_status_on", "differential"))
Line2_Pred <- RxXdfData("Line2_Pred_Test.xdf")


Stats_Calculator <- function(xdf){
  
  xdf <- mutate(xdf, residuals = hhpu - hhpu_Pred)
  hhpu_mean <- mean(xdf$hhpu)
  SSres <- sum(xdf$residuals^2)
  SStot <- sum((xdf$hhpu - hhpu_mean)^2)
  p <- length(names(xdf)) - 2
  n <- nrow(xdf)
  
  MAE <- sum(abs(xdf$residuals))/n
  RMSE <- sqrt(sum(xdf$residuals^2)/n)
  Rsquared <- 1 - SSres/SStot
  Rsquared_adj <- Rsquared - (1 - Rsquared)*(p/(n-p-1))
  
  return(c(MAE,RMSE,Rsquared,Rsquared_adj))
  
}

eval <- Stats_Calculator(Line2_Pred)







t <- as.POSIXct("2016-01-01 07:00:00", tz = "UTC")
tt <- with_tz(t, tzone = "MST")

rxDataStep(Line2, outFile = "Line21.xdf", varsToDrop = c("ts_month", "ts_dow", "ts_day", "ts_hour"))

create_DatetimeVars <- function(dat){
  tz(dat$ts_pi) <- "UTC"
  dat$ts_pi <- with_tz(dat$ts_pi, tzone = "MST")
  dat$ts_month <- factor(month(dat$ts_pi), levels = as.character(1:12))
  dat$ts_dow <- as.factor(weekdays(dat$ts_pi, abbreviate = T))
  dat$ts_day <- as.factor(day(dat$ts_pi))
  dat$ts_hour <- as.factor(hour(dat$ts_pi))
  return(dat)
}

rxDataStep("Line21.xdf", outFile = "Line22.xdf", transformFunc = create_DatetimeVars, transformPackages = "lubridate")
Line2 <- change_Xdf("Line2.xdf", n = 2)
