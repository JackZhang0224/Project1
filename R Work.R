df <- read.csv("Line2_Month02_2016.csv.bz2")

class <- c("LogTime" = "character", "Id" = "integer", "Seg" = "factor", "Site" = "factor", "TagType" = "character",
           "ValType" = "factor", "ValInt" = "numeric", "ValFlt" = "numeric", "ValStr" = "character", "ValDig" = "character",
           "ValTStamp" = "character", "IsGood" = "factor", "IsQuestionable" = "factor", "uom" = "factor",
           "LogMonth" = "factor", "LogYear" = "factor")

azureStorageCall <- function(url, verb, key, storageType="blob", requestBody=NULL, headers=NULL, ifMatch="", md5="")

azureCSVFileToDataFrame <- function (azureStorageUrl, key, container, csvFilepath, Md5, storageType = "blob", encoding = 'UTF-8') {
  
  # Replace spaces with HTTP encoding, i.e., %20
  container <- gsub(" ", "%20", container)  
  csvFilepath <- gsub(" ", "%20", csvFilepath)  
  
  # Assemble the URL
  azureUrl <- paste0(azureStorageUrl, "/", container, "/", csvFilepath) 
  
  # Get the file from the Azure Storage Account
  response <- azureStorageCall(url = azureUrl, verb = "GET", key = key, storageType = storageType, md5 = Md5)  
  
  # Get the content of the response.  We are expecting "text".
  csv <- content(response, as="text", encoding = encoding)
  
  # Save the csv content to a temporary file.
  tmp_csv_filename <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv")
  write(csv, tmp_csv_filename, ncolumns = 1, sep = "")
  
  # Remove the csv variable to save memory
  remove(csv)
  
  # Read the csv file into a data frame.
  df <- read.csv(tmp_csv_filename)
  
  # Remove the temporary file.
  file.remove(tmp_csv_filename)  
  
  return (df)
}

url1 <- "https://enbze2elpdestarserver.blob.core.windows.net/2017-power-optimization/machine_learning/data/2016/Line2_Month02_2016.csv.bz2"
container <- "2017-power-optimization"
zipFilepath <- "machine_learning/data/2016/Line2_Month02_2016.csv.bz2"
azureStorageUrl <- "https://enbze2elpdestarserver.blob.core.windows.net"

container <- gsub(" ", "%20", container)
zipFilepath <- gsub(" ", "%20", zipFilepath)
url <- paste0(azureStorageUrl, "/", container, "/", zipFilepath)

response <- azureStorageCall(url, "GET", AzurePrimkey, storageType = "blob")
csv <- content(response, as="text", encoding = encoding)






class <- c("LogTime" = "character", "Id" = "integer", "Seg" = "factor", "Site" = "factor", "TagType" = "character",
           "ValType" = "factor", "ValInt" = "numeric", "ValFlt" = "numeric", "ValStr" = "character",
           "ValDig" = "character", "ValTStamp" = "character", "IsGood" = "factor", "IsQuestionable" = "factor",
           "uom" = "character", "LogMonth" = "factor", "LogYear" = "factor")


sdf <- rxDataStep(Line21, rowSelection = (u < .1), transforms = list(u = runif(.rxNumRows)))


create_Value1 <- function(dat){
  for (i in 1:nrow(dat)){
    if (dat[i,5] == "int") {dat[i,5] <- as.character(dat[i,6])}
    else if (dat[i,5] == "float") {dat[i,5] <- as.character(dat[i,7])}
    else {dat[i,5] <- dat[i,8]}
  }
  return(dat)
}

create_Value2 <- function(x){
  if (x == "int") {x <- as}
}

Line2_int <- rxDataStep(Line2, outFile = "Line2_int.xdf", rowSelection = (ValType == "int"), overwrite = T,
                        transforms = list(Value = as.character(ValInt)))
rename(Line2_int,c("Value"="ValInt"))
rxDataStep(Line2_int, outFile = "Line2_int1.xdf", overwrite = T, varsToDrop = c("ValType","ValInt","ValFlt","ValDig"))
file.remove("Line2_int.xdf")
file.rename("Line2_int1.xdf","Line2_int.xdf")
Line2_int <- RxXdfData("Line2_int.xdf")


Conv_Str <- function(dat){
  dat$Value = as.character(dat$ValInt)
  return(dat)
}

Line2_int <- rxDataStep(Line2_int, outFile = "Line2_int.xdf", overwrite = T,
                        transforms = list(Value = as.character(ValInt)))


Site_df <- rxImport(inData = Line2, rowSelection = (Site == S[1]), transformObjects = list(S = Sites))


for (i in 1:length(Sites)){
  Site_xdf <- rxDataStep(inData = Line21, rowSelection = (Site == S[i]), transformObjects = list(S = Sites))
}


library(FSelector)
Line2_df <- rxImport(Line2)
Line2_df <- Line2_df[-c(1,8,15,17,18)]
corr <- information.gain(hhpu~.,Line2_df)
col_names <- names(Line2_df)
col_names <- col_names[-c(6)]
corr <- data.frame("Features" = col_names, "Information_Gain" = corr$attr_importance)
corr <- corr[order(corr$Information_Gain, decreasing = T),]


Rename segments 2-CM-GF and 2-GF-CR to 2-CM-CR. We'll need to convert DESTINATION_FCLTY_NAME to a character and then back to a factor afterwards for this to work.

```{r Rename Segments, message = FALSE}

segment_rename <- function(dat) {
dat$DESTINATION_FCLTY_NAME <- as.character(dat$DESTINATION_FCLTY_NAME)
dat$DESTINATION_FCLTY_NAME <- ifelse(dat$DESTINATION_FCLTY_NAME == "2-CM-GF", "2-CM-CR",
ifelse(dat$DESTINATION_FCLTY_NAME == "2-GF-CR", "2-CM-CR",
dat$DESTINATION_FCLTY_NAME))
return(dat)
}

rxDataStep(Schedule, outFile = "Schedule_Linefill1.xdf", overwrite = T, transformFunc = segment_rename)
rxFactors("Schedule_Linefill1.xdf", outFile = "Schedule_Linefill2.xdf", overwrite = T,
factorInfo = c("DESTINATION_FCLTY_NAME"))
file.remove("Schedule_Linefill.xdf")
file.remove("Schedule_Linefill1.xdf")
file.rename("Schedule_Linefill2.xdf","Schedule_Linefill.xdf")
Schedule <- RxXdfData("Schedule_Linefill.xdf")

```
