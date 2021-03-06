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
## Holt-Winters
##
###############################

## Dataframe de teste
dados.test.hw <- tail(dados.ts.na.removed, n = 100)

## Dataframe de treino
dados.train.hw <- head(dados.ts.na.removed, n = (length(dados.ts.na.removed) - 100))

ajuste.holt <- HoltWinters(dados.train.hw)
plot(dados.ts.na.removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')

## Previsao usando o Holt-Winters
holt.forecast <- forecast(ajuste.holt, h = 100, level = 95)
#plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")

## Plotagem comparando o treino com o teste
plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
lines(dados.ts.na.removed, lwd = 2, col = 'green')
legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')

## Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
dados.fore <- forecast(dados.ts.na.removed, h=10)
dados.fore

## gráfico de autocorrelação
plot(acf(dados.ts.na.removed))

### FUNCAO PARA SER APLICADA NO SPARK ###

run_holtwinters <- function(df) {
  ## Dataframe de teste
  dados.test.hw <- tail(df, n = 100)
  
  ## Dataframe de treino
  dados.train.hw <- head(df, n = (length(df) - 100))
  
  ajuste.holt <- HoltWinters(dados.train.hw)
  plot(df, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
  lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
  legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')
  
  ## Previsao usando o Holt-Winters
  holt.forecast <- forecast(ajuste.holt, h = 100, level = 95)
  #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  
  ## Plotagem comparando o treino com o teste
  plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  lines(df, lwd = 2, col = 'green')
  legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
  
  ## Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
  dados.fore <- forecast(df, h=10)
  dados.fore
  
  ## gráfico de autocorrelação
  plot(acf(df))
}

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

# ## Testar com configurações de arima, variando o 'p', 'd' e 'q' entre 0 e 1
# apply(config_list, 1, verificar_arimas, orig_df = dados.ts.na.removed)
# 
# ## Dataframe de teste
# dados.test <- tail(dados.ts.na.removed, n = 100)
# 
# ## Dataframe de treino
# dados.train <- head(dados.ts.na.removed, n = (length(dados.ts.na.removed) - 100))
# 
# ## Dataframe de predições
# df.pred <- data.frame(pred = numeric(0))
# 
# ## Ajustar o train para ser usado no loop
# history <- data.frame(pred = numeric(0))
# 
# for (i in dados.train) {
#   history <- rbind(history, i)
# }
# 
# for (i in dados.test) {
#   #model_fit <- arima(history, c(1, 0, 0))
#   model_fit <- arima(history, c(1, 1, 2), list(order = c(2, 0, 2)))
#   output <- forecast(model_fit, h = 1)
#   df.pred <- rbind(df.pred, c(output$mean[1]))
#   history <- rbind(history, i)
#   print(sprintf("predicted: %s <> expected: %s", output$mean[1], i))
# }
# 
# ## Plot
# plot(forecast(fit.power, h = 100, level = 0.95), xlab = "tempo", ylab = "Valores observados/previstos", main = "")
# lines(dados.ts.na.removed, lwd = 2, col = 'green')
# legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
# 
# #print(MSE(df.pred$X0.0306351443186633, dados.test))
# 
# ## Realizando os testes para verificar qual melhor ARIMA a ser aplicado
# autoarima <- auto.arima(dados.ts.na.removed, trace=TRUE)
# autoarima

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

# for (i in dados.test) {
#   #model_fit <- arima(history, c(1, 0, 0))
#   model_fit <- arima(history, c(1, 1, 2), list(order = c(2, 0, 2)))
#   output <- forecast(model_fit, h = 10)
#   df.pred <- rbind(df.pred, c(output$mean[1]))
#   history <- rbind(history, i)
#   #print(sprintf("predicted: %s <> expected: %s", output$mean[1], i))
# }

fit.power <- Arima(dados.train, order = c(1, 1, 2),
                   seasonal = c(2, 0, 2))

## Teste Ljung-Box para testar a autocorrelacao linear dos residuos
Box.test(x = fit.power$residuals, lag = 24, type = "Ljung-Box", fitdf = 2)


## Testar normalidade dos residuos
#jb.norm.test(fit.power$residuals, nrepl = 10)

## Plot
plot(forecast(fit.power, h = 100, level = 0.95), xlab = "tempo", ylab = "Valores observados/previstos", main = "")
lines(dados.ts.na.removed, lwd = 2, col = 'green')
legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
