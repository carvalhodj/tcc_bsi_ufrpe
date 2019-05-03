library(anytime)
library(forecast)
library(urca)
library(tseries)
library(MLmetrics)

setwd("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/csv_files/")

## Filenames
ORIG_FILENAME <- "filtered0010.csv"
TIME_FILENAME <- "filtered0010-tempo.csv"
AGREG_FILENAME <- "filtered0010-consumo_agregado.csv"

## PRIMEIRA PARTE
dados <- read.csv(ORIG_FILENAME, header=FALSE)
dados$tempo <- anytime(dados$V1)
dados$tempo
write.csv(dados, TIME_FILENAME)
## SEGUNDA PARTE
dados <- read.csv(TIME_FILENAME)
dados$datetime <- as.POSIXct(as.numeric(as.character(dados$V1)),origin="1970-01-01",tz="GMT")
dados$Datetime2 <- droplevels(cut(dados$datetime, breaks="hours"))
agregado = aggregate(V2 ~ Datetime2, data=dados, FUN=function(x) x[length(x)]-x[1]) # https://blogs.ubc.ca/yiwang28/2017/05/04/my-r-learning-notes-quick-ways-to-aggregate-minutely-data-into-hourly-data/
write.csv(agregado, AGREG_FILENAME)
## TERCEIRA PARTE
dados <- read.csv("filtered0010-consumo_agregado.csv")
plot(dados$V2)