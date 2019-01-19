library(anytime)
library(forecast)
library(data.table)
library(zoo)
library(xts)
library(urca)
library(tseries)

setwd("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/csv_files/")

# PRIMEIRA PARTE
# dados <- read.csv("sorted_tempo.csv", header=FALSE)
# dados$tempo <- anytime(dados$V1)
# dados$tempo
# write.csv(dados, "sorted_tempo.csv")
# SEGUNDA PARTE
# dados <- read.csv("sorted_tempo.csv")
# dados$datetime <- as.POSIXct(as.numeric(as.character(dados$V1)),origin="1970-01-01",tz="GMT")
# dados$Datetime2 <- droplevels(cut(dados$datetime, breaks="hours"))
# agregado = aggregate(V2 ~ Datetime2, data=dados, FUN=function(x) x[length(x)]-x[1]) # https://blogs.ubc.ca/yiwang28/2017/05/04/my-r-learning-notes-quick-ways-to-aggregate-minutely-data-into-hourly-data/
# write.csv(agregado, "debs_consumo_agregado.csv")
# TERCEIRA PARTE
dados <- read.csv("debs_consumo_agregado.csv")
#consumo <- read.zoo(file="debs_consumo_agregado.csv", sep=",", header=TRUE,
#                    index=2, tz="GMT", format="%Y-%m-%d %H:%M:%S")
idx <- as.Date(dados$Datetime2)
dados$datetime <- idx
x <- ts(dados$V2, frequency=24)
y <- decompose(x)
# Teste para verificar se a serie é estacionaria
z <- summary(ur.kpss(x))
# Tratamento necessário para realizar o forecast
a <- na.remove(x)
# Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
fore <- forecast(a, h=10)
fore
# gráfico de autocorrelação
plot(acf(a))
# Realizando os testes para verificar qual melhor ARIMA a ser aplicado
autoarima <- auto.arima(a, trace=TRUE)
autoarima
