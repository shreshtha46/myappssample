library(data.table)
library(sqldf)
library(rworldmap)
library(RColorBrewer)
library(ggplot2)
library(dplyr)

setwd('D:/Semester 2/Scalable/Assignment 1\NZ_dailyTminTmaxData.csv')
folder <- "D:/Semester 2/Scalable/Assignment 1\NZ_dailyTminTmaxData.csv"
file_list <- list.files(path=folder, pattern="*.csv")
data <- 
  do.call("rbind",
          lapply(file_list,
                 function(x)
                   read.csv(paste(folder, x, sep=''),
                            stringsAsFactors = FALSE)))
data_sum <- data[,1:4]
data_sum$DATE = as.character(data_sum$DATE)

data_sum["Format_Date"] <- as.Date(data_sum[["DATE"]], "%Y")
data_sum["Year"] <- substring(data_sum$DATE,1,4)
data_sum$Year = as.numeric(data_sum$Year)
data_sum <- data_sum[order(as.Date(data_sum$Format_Date, format="%Y%m%d")),]


DT <- data.table(data_sum, key = "ID")

data_sum_tmax <- sqldf("select ID,Year, AVG(VALUE) as avg_tmax from DT where ELEMENT='TMAX' group by Year,ID")
data_sum_tmin <- sqldf("select ID,Year, AVG(VALUE) as avg_tmin from DT where ELEMENT='TMIN' group by Year,ID")
ggplot(data_sum_tmax, aes(x = Year, y = avg_tmax, colour= ID, group= ID)) + 
  geom_line() +
  theme_minimal()+
  ggtitle("Average TMAX in degrees for New Zealand Stations")


ggplot(data_sum_tmin, aes(x = Year, y = avg_tmin, colour= ID, group= ID)) + 
  geom_line() +
  theme_minimal()+
  ggtitle("Average TMIN in degrees for New Zealand Stations")


data_sum_country_max <- sqldf("Select Year, AVG(VALUE) as tmax_cntry from DT where ELEMENT='TMAX'  group by Year")
data_sum_country_min <- sqldf("Select Year, AVG(VALUE) as tmin_cntry from DT where ELEMENT='TMIN' group by Year")

data_sum_country <- merge(data_sum_country_max,data_sum_country_min,by="Year")

ggplot(data_sum_country, aes(x = Year)) + 
  geom_line(aes(y= tmax_cntry, color="TMAX")) +
  geom_line(aes(y= tmin_cntry, color="TMIN")) +
  theme_minimal()+
  ggtitle("Average TMAX and TMIN in degrees for New Zealand")
