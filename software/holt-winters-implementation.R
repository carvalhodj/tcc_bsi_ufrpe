run_hw <- function(in_coluna_diferenca) {
  ## Libs
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(stats)
  
  # HOUSE_ID <- in_coluna_diferenca[length(in_coluna_diferenca)]
  FORECAST_WINDOW <- 72
  HEADER_NAME <- "all"
  
  ## Remove o indice do vetor
  # in_coluna_diferenca <- head(in_coluna_diferenca, n = (length(in_coluna_diferenca) - 1))
  
  ## Eliminar outliers
  qnt <- quantile(in_coluna_diferenca, probs=c(.25, .75), na.rm = TRUE)
  caps <- quantile(in_coluna_diferenca, probs=c(.05, .95), na.rm = T)
  H <- 1.5 * IQR(in_coluna_diferenca, na.rm = TRUE)
  coluna_diferenca <- in_coluna_diferenca
  coluna_diferenca[in_coluna_diferenca < (qnt[1] - H)] <- caps[1]
  coluna_diferenca[in_coluna_diferenca > (qnt[2] + H)] <- caps[2]
  
  ## Criando o objeto timeseries
  dados_ts <- ts(coluna_diferenca, frequency=24)
  
  ## Arquivo do gráfico
  name_graphs <- paste("hw_graph", HEADER_NAME, 
                       sep = "-",
                       collapse = " ")
  
  name_pdf <- paste(name_graphs, "pdf", sep = ".")
  
  pdf(name_pdf)
  
  Acf(dados_ts, lag.max = 20)
  Pacf(dados_ts, lag.max = 20)
  
  ## Tratamento necessário para realizar o forecast
  dados_ts_na_removed <- na.approx(dados_ts)
  
  ## Retirando a parte final do dataframe que compromete o teste
  dados_ts_na_removed <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 144))
  
  ## Dataframe de teste
  dados_test <- tail(dados_ts_na_removed, n = FORECAST_WINDOW)
  
  ## Dataframe de treino
  dados_train <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - FORECAST_WINDOW))
  
  ajuste_holt <- HoltWinters(dados_train)
  
  plot(fitted(ajuste_holt))
  
  ## Coeficientes alpha, beta e gamma
  write.table(HEADER_NAME, 
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("alpha:", ajuste_holt$alpha), 
                    sep = " ", 
                    collapse = " "), 
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("beta:", ajuste_holt$beta), 
                    sep = " ", 
                    collapse = " "),
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("gamma:", ajuste_holt$gamma), 
                    sep = " ", 
                    collapse = " "),
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table("======",
              file = "ajuste_holt.txt",
              append = TRUE)
  
  plot(dados_ts_na_removed, xlab = 'Dias', ylab = 'Valores reais/ajustados (Wh)', main = '')
  lines(fitted(ajuste_holt)[,1], lwd = 2, col = 'red')
  legend("topright", c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'o')
  
  ## Previsao usando o Holt-Winters
  holt_forecast <- forecast(ajuste_holt, h = FORECAST_WINDOW, level = 95, lambda = T)
  
  write.table(HEADER_NAME, 
              file = "holt_forecast.txt",
              append = TRUE)
  write.table(holt_forecast,
              file = "holt_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "holt_forecast.txt",
              append = TRUE)
  
  ## Plotagem comparando o treino com o teste
  plot(holt_forecast, xlab = "Dias", 
       ylab = "Valores reais/previstos (Wh)", 
       main = "",
       ylim = c(-20000, 20000))
  lines(dados_ts_na_removed, lwd = 2, col = 'green')
  legend("bottomleft", c("Real", "Previsto"), lwd = c(1, 2), 
         col = c("green", "blue"), bty = 'o')
  
  dev.off()
  
  ## Calculo dos indices de erro
  indices <- accuracy(holt_forecast, 
                      x = dados_test)
  
  write.table(HEADER_NAME, 
              file = "indices_hw.txt",
              append = TRUE)
  write.table(indices,
              file = "indices_hw.txt",
              append = TRUE)
  write.table("======",
              file = "indices_hw.txt",
              append = TRUE)
}

###############################
###############################

run_hw_df <- function(df) {
  ## Libs
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(stats)
  
  setwd("/home/d3jota/UFRPE/BSI/TCC/pos/")
  
  FORECAST_WINDOW <- 72
  
  ## HEADER_NAME PARA O CENÁRIO DE PREVISÃO
  ## POR RESIDÊNCIA
  HEADER_NAME <- paste(c("house:", df$house_id[1]), 
                       sep = " ", 
                       collapse = " ")
  
  ## CASO SEJA O CONSUMO DA REGIÃO
  ## HEADER_NAME <- "all"
  
  ## Eliminar outliers
  qnt <- quantile(df$total, probs=c(.25, .75), na.rm = TRUE)
  caps <- quantile(df$total, probs=c(.05, .95), na.rm = TRUE)
  H <- 1.5 * IQR(df$total, na.rm = TRUE)
  df$total[df$total < (qnt[1] - H)] <- caps[1]
  df$total[df$total > (qnt[2] + H)] <- caps[2]
  coluna_diferenca <- df$total

  
  ## Criando o objeto timeseries
  dados_ts <- ts(coluna_diferenca, frequency=24*7)
  
  ## Arquivo do gráfico
  name_graphs <- paste("hw_graph", df$house_id[1], 
                       sep = "-",
                       collapse = " ")
  
  name_pdf <- paste(name_graphs, "pdf", sep = ".")
  
  Acf(dados_ts, lag.max = 20)
  Pacf(dados_ts, lag.max = 20)
  
  pdf(name_pdf)
  
  ## Tratamento necessário para realizar o forecast
  dados_ts_na_removed <- na.approx(dados_ts)
  
  ## Retirando a parte final do dataframe que compromete o teste
  dados_ts_na_removed <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 144))
  
  ## Dataframe de teste
  dados_test <- tail(dados_ts_na_removed, n = FORECAST_WINDOW)
  
  ## Dataframe de treino
  dados_train <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - FORECAST_WINDOW))
  
  ajuste_holt <- HoltWinters(dados_train)
  
  plot(fitted(ajuste_holt))
  
  ## Coeficientes alpha, beta e gamma
  write.table(HEADER_NAME, 
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("alpha:", ajuste_holt$alpha), 
                    sep = " ", 
                    collapse = " "), 
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("beta:", ajuste_holt$beta), 
                    sep = " ", 
                    collapse = " "),
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table(paste(c("gamma:", ajuste_holt$gamma), 
                    sep = " ", 
                    collapse = " "),
              file = "ajuste_holt.txt",
              append = TRUE)
  
  write.table("======",
              file = "ajuste_holt.txt",
              append = TRUE)
  
  plot(dados_ts_na_removed, xlab = 'Dias', ylab = 'Valores reais/ajustados (Wh)', main = '')
  lines(fitted(ajuste_holt)[,1], lwd = 2, col = 'red')
  legend("topright", c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'o')
  
  ## Previsao usando o Holt-Winters
  holt_forecast <- forecast(ajuste_holt, h = FORECAST_WINDOW, level = 95)
  
  write.table(HEADER_NAME, 
              file = "holt_forecast.txt",
              append = TRUE)
  write.table(holt_forecast,
              file = "holt_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "holt_forecast.txt",
              append = TRUE)
  
  ## Plotagem comparando o treino com o teste
  plot(holt_forecast, xlab = "Dias", ylab = "Valores reais/previstos (Wh)", main = "")
  lines(dados_ts_na_removed, lwd = 2, col = 'green')
  legend("bottomleft", c("Real", "Previsto"), lwd = c(1, 2), 
         col = c("green", "blue"), bty = 'o')
  
  dev.off()
  
  ## Calculo dos indices de erro
  indices <- accuracy(holt_forecast, 
                      x = dados_test)
  
  write.table(HEADER_NAME, 
              file = "indices_hw.txt",
              append = TRUE)
  write.table(indices,
              file = "indices_hw.txt",
              append = TRUE)
  write.table("======",
              file = "indices_hw.txt",
              append = TRUE)
  
}