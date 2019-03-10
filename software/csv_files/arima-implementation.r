library(anytime)
library(forecast)
library(urca)
library(tseries)
library(MLmetrics)
library(data.table)
library(zoo)

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
dados <- read.csv("filtered0010-consumo_agregado.csv")

## Remover outliers
outlierReplace = function(dataframe, cols, rows, newValue = NA) {
  if (any(rows)) {
    set(dataframe, rows, cols, newValue)
  }
}

## Substituir os outliers por NA
outlierReplace(dados, "V2", 
               which(dados$V2 > 1), NA)

## Substituir os NAs pela media do valor anterior e posterior
dados$V2 <- na.approx(dados$V2)

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
dados.teste.estacionariedade

## Tratamento necessário para realizar o forecast
dados.ts.na.removed <- na.remove(dados.ts)

###############################
##
## ARIMA
##
###############################

## Realizando o cálculo do erro médio quadrático de cada configuração
verificar_arimas <- function(x, orig_df) {
  configuracao <- c(x[1], x[2], x[3])
  ## Dataframe de teste
  dados.test <- tail(orig_df, n = 100)
  
  ## Dataframe de treino
  dados.train <- head(orig_df, n = (length(orig_df) - 100))
  
  ## Dataframe de predições
  df.pred <- data.frame(pred = numeric(0))
  
  ## Ajustar o train para ser usado no loop
  history <- data.frame(pred = numeric(0))
  
  for (i in dados.train) {
    history <- rbind(history, i)
  }
  
  for (i in dados.test) {
    #model_fit <- arima(history, c(1, 0, 0))
    model_fit <- arima(history, configuracao)
    output <- forecast(model_fit, h = 1)
    df.pred <- rbind(df.pred, c(output$mean[1]))
    history <- rbind(history, i)
    print(x)
    print(sprintf("predicted: %s <> expected: %s", output$mean[1], i))
  }
  print("### calculo do erro ###")
  print(MSE(df.pred[, 1], dados.test))
}

## Criar lista de vetores a serem usados como parametro no arima()
config_list <- matrix(
  c(0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 1, 1, 1, 1),
  nrow = 7,
  ncol = 3,
  byrow = TRUE
)

## Testar com configurações de arima, variando o 'p', 'd' e 'q' entre 0 e 1
# apply(config_list, 1, verificar_arimas, orig_df = dados.ts.na.removed)

## Dataframe de teste
dados.test <- tail(dados.ts.na.removed, n = 100)

## Dataframe de treino
dados.train <- head(dados.ts.na.removed, n = (length(dados.ts.na.removed) - 100))

## Dataframe de predições
df.pred <- data.frame(pred = numeric(0))

## Ajustar o train para ser usado no loop
history <- data.frame(pred = numeric(0))

for (i in dados.train) {
  history <- rbind(history, i)
}

for (i in dados.test) {
  #model_fit <- arima(history, c(1, 0, 0))
  model_fit <- arima(history, c(1, 1, 2), list(order = c(2, 0, 2)))
  output <- forecast(model_fit, h = 10)
  df.pred <- rbind(df.pred, c(output$mean[1]))
  history <- rbind(history, i)
  #print(sprintf("predicted: %s <> expected: %s", output$mean[1], i))
}

plot(output)

# print(MSE(df.pred$X0.0306351443186633, dados.test))

## Realizando os testes para verificar qual melhor ARIMA a ser aplicado
# autoarima <- auto.arima(dados.ts.na.removed, trace=TRUE)
# autoarima