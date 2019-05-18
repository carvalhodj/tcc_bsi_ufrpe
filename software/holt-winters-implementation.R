# library(anytime)
# library(forecast)
# library(urca)
# library(tseries)
# library(MLmetrics)
# library(data.table)
# library(zoo)
# 
# dados <- read.csv("/home/d3jota/teste/house_5/house_5.csv")
# 
# ## Remover outliers
# outlierReplace = function(dataframe, cols, rows, newValue = NA) {
#   if (any(rows)) {
#     set(dataframe, rows, cols, newValue)
#   }
# }
# 
# ## Substituir os outliers por NA
# outlierReplace(dados, "diferenca", 
#                which(dados$diferenca > 1), NA)
# 
# ## Substituir os NAs pela media do valor anterior e posterior
# dados$diferenca <- na.approx(dados$diferenca)
# 
# ## Criando uma nova coluna de objetos 'Date', convertendo da coluna
# ## preexistente de datas, que estao no formato string
# #idx <- as.Date(dados$Datetime2)
# #dados$datetime <- idx
# 
# ## Criando a serie temporal com os dados lidos do csv
# ## Frequencia de 24 pois sao dados diarios medidos de hora em hora
# dados.ts <- ts(dados$diferenca, frequency=24)
# 
# ## Decompondo a serie temporal a fim de verificar cada componente
# dados.decomposed <- decompose(dados.ts)
# plot(dados.decomposed)
# 
# ## Teste para verificar se a serie é estacionaria
# ## O 'diff' é para torna-la estacionaria
# dados.teste.estacionariedade <- summary(ur.kpss(diff(diff(dados.ts))))
# dados.teste.estacionariedade
# 
# dados.ts <- diff(diff(dados.ts))
# 
# ## Tratamento necessário para realizar o forecast
# dados.ts.na.removed <- na.remove(dados.ts)
# 
# ###############################
# ##
# ## Holt-Winters
# ##
# ###############################
# 
# ## Dataframe de teste
# dados.test.hw <- tail(dados.ts.na.removed, n = 100)
# 
# ## Dataframe de treino
# dados.train.hw <- head(dados.ts.na.removed, n = (length(dados.ts.na.removed) - 100))
# 
# ajuste.holt <- HoltWinters(dados.train.hw)
# plot(dados.ts.na.removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
# lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
# legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')
# 
# ## Previsao usando o Holt-Winters
# holt.forecast <- forecast(ajuste.holt, h = 100, level = 95)
# #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
# 
# ## Plotagem comparando o treino com o teste
# plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
# lines(dados.ts.na.removed, lwd = 2, col = 'green')
# legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
# 
# ## Calculo dos indices de erro
# accuracy(holt.forecast, x = dados.test.hw)
# 
# ## gráfico de autocorrelação
# plot(acf(dados.ts.na.removed))
# 
# ### FUNCAO PARA SER APLICADA NO SPARK ###
# 
# run_holtwinters <- function(df) {
#   ## Dataframe de teste
#   dados.test.hw <- tail(dados.ts.na.removed, n = 100)
#   
#   ## Dataframe de treino
#   dados.train.hw <- head(dados.ts.na.removed, n = (length(dados.ts.na.removed) - 100))
#   
#   ajuste.holt <- HoltWinters(dados.train.hw)
#   plot(dados.ts.na.removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
#   lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
#   legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')
#   
#   ## Previsao usando o Holt-Winters
#   holt.forecast <- forecast(ajuste.holt, h = 100, level = 95)
#   #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
#   
#   ## Plotagem comparando o treino com o teste
#   plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
#   lines(dados.ts.na.removed, lwd = 2, col = 'green')
#   legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
#   
#   ## Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
#   dados.fore <- forecast(dados.ts.na.removed, h=10)
#   dados.fore
#   
#   ## gráfico de autocorrelação
#   plot(acf(dados.ts.na.removed))
#   
#   # print("### calculo do erro ###")
#   # print(MSE(holt.forecast[, 1], dados.test.hw))
# }

################################
################################
################################

run_hw <- function(coluna_diferenca) {
  ## Libs
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(stats)

  ## Criando o objeto timeseries
  dados_ts <- ts(coluna_diferenca, frequency=24)

  ## Verificando a estacionariedade
  dados_teste_estacionariedade <- summary(ur.kpss(dados_ts))
  
  # print(dados_teste_estacionariedade)
  # write.table(dados_teste_estacionariedade,
  #             file = "dados_teste_estacionariedade.txt",
  #             append = TRUE)
  # write.table("============================",
  #             file = "dados_teste_estacionariedade.txt",
  #             append = TRUE)
  
  ## Tratamento necessário para realizar o forecast
  dados_ts_na_removed <- na.remove(dados_ts)

  ## Dataframe de teste
  dados_test_hw <- tail(dados_ts_na_removed, n = 72)

  ## Dataframe de treino
  dados_train_hw <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 72))

  ajuste_holt <- HoltWinters(dados_train_hw)
  write.table(holt_forecast,
              file = "ajuste_holt.txt",
              append = TRUE)
  write.table("======",
              file = "ajuste_holt.txt",
              append = TRUE)
  
  # plot(dados_ts_na_removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
  # lines(fitted(ajuste_holt)[,1], lwd = 2, col = 'red')
  # legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')

  ## Previsao usando o Holt-Winters
  holt_forecast <- forecast(ajuste_holt, h = 72, level = 95)
  #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  
  write.table(holt_forecast,
              file = "holt_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "holt_forecast.txt",
              append = TRUE)
  
  ## Plotagem comparando o treino com o teste
  # plot(holt_forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  # lines(dados_ts_na_removed, lwd = 2, col = 'green')
  # legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
  # print(holt_forecast)

  ## Calculo dos indices de erro
  indices <- accuracy(holt_forecast, x = dados_test_hw)
  
  write.table(indices,
              file = "indices_hw.txt",
              append = TRUE)
  write.table("======",
              file = "indices_hw.txt",
              append = TRUE)
  
}