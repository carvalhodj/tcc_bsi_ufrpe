library(anytime)
library(forecast)
#library(data.table)
#library(zoo)
#library(xts)
library(urca)
library(tseries)

setwd("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/csv_files/")

## PRIMEIRA PARTE
# dados <- read.csv("sorted_tempo.csv", header=FALSE)
# dados$tempo <- anytime(dados$V1)
# dados$tempo
# write.csv(dados, "sorted_tempo.csv")
## SEGUNDA PARTE
# dados <- read.csv("sorted_tempo.csv")
# dados$datetime <- as.POSIXct(as.numeric(as.character(dados$V1)),origin="1970-01-01",tz="GMT")
# dados$Datetime2 <- droplevels(cut(dados$datetime, breaks="hours"))
# agregado = aggregate(V2 ~ Datetime2, data=dados, FUN=function(x) x[length(x)]-x[1]) # https://blogs.ubc.ca/yiwang28/2017/05/04/my-r-learning-notes-quick-ways-to-aggregate-minutely-data-into-hourly-data/
# write.csv(agregado, "debs_consumo_agregado.csv")
## TERCEIRA PARTE
dados <- read.csv("debs_consumo_agregado.csv")

## Criando uma nova coluna de objetos 'Date', convertendo da coluna
## preexistente de datas, que estao no formato string
#idx <- as.Date(dados$Datetime2)
#dados$datetime <- idx

## Criando a serie temporal com os dados lidos do csv
## Frequencia de 24 pois sao dados diarios medidos de hora em hora
dados.ts <- ts(dados$V2, frequency=24)

## Decompondo a serie temporal a fim de verificar cada componente
dados.decomposed <- decompose(dados.ts)
plot(dados.decomposed)

## Teste para verificar se a serie é estacionaria
## O 'diff' é para torna-la estacionaria
dados.teste.estacionariedade <- summary(ur.kpss(diff(dados.ts)))

## Tratamento necessário para realizar o forecast
dados.ts.na.removed <- na.remove(dados.ts)

## Holt-Winters
ajuste.holt <- HoltWinters(dados.ts.na.removed, gamma = FALSE)
plot(dados.ts.na.removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')

## Previsao usando o Holt-Winters
holt.forecast <- forecast(ajuste.holt, h = 10, level = 95)
plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")

## Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
dados.fore <- forecast(dados.ts.na.removed, h=10)
dados.fore

## gráfico de autocorrelação
plot(acf(a))

## Realizando os testes para verificar qual melhor ARIMA a ser aplicado
autoarima <- auto.arima(a, trace=TRUE)
autoarima
