library(data.table)
library(sqldf)
library(rworldmap)
library(RColorBrewer)

setwd('E:\\SecondSem\\Scalable\\assignment\\Code\\precipitation')
folder <- "E:/SecondSem/Scalable/assignment/Code/precipitation/"
file_list <- list.files(path=folder, pattern="*.csv")
data <- 
  do.call("rbind",
          lapply(file_list,
                 function(x)
                   read.csv(paste(folder, x, sep=''),
                            stringsAsFactors = FALSE)))


DT <- data.table(data, key = "COUNTRY_NAME")

#write.csv(DT,'data.csv')
#df$csum <- ave(df$value, df$id, FUN=cumsum)
DT$csum_rain <- ave(DT$avg_prec, DT$COUNTRY_NAME, FUN=cumsum)
View(DT)

data11 <- sqldf("select max(csum_rain) as cumm_rainfall, COUNTRY_NAME, count(csum_rain) as count from DT group by COUNTRY_NAME")
data_final <- sqldf("select (cumm_rainfall/count) as average_rainfall,COUNTRY_NAME from data11")
  
  

data_map <- joinCountryData2Map(data_final, joinCode = "NAME", nameJoinColumn = "COUNTRY_NAME")

colorPallete <- brewer.pal(9,'Blues')
mapCountryData(data_map,
               nameColumnToPlot = 'average_rainfall',
               catMethod = 'fixedWidth',
               numCats = 9, colourPalette = colorPallete,
               borderCol = "black", mapTitle = "Average rainfall in tenths of mm")
