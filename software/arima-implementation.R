run_arima <- function(in_column_difference) {
  ## Libs necessarias
  require(anytime)
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(normtest)
  
  # HOUSE_ID <- tail(in_column_difference, n = 1)
  FORECAST_WINDOW <- 72
  HEADER_NAME <- "all"
  
  ## Remove o indice do vetor
  # in_column_difference <- head(in_column_difference, n = (length(in_column_difference) - 1))
  
  ## Eliminar outliers
  qnt <- quantile(column_difference, probs=c(.25, .75), na.rm = T)
  caps <- quantile(column_difference, probs=c(.05, .95), na.rm = T)
  H <- 1.5 * IQR(column_difference, na.rm = T)
  column_difference[column_difference < (qnt[1] - H)] <- caps[1]
  column_difference[column_difference > (qnt[2] + H)] <- caps[2]
  
  dados_ts <- ts(column_difference, frequency=24)
  
  ## Arquivo do gráfico
  name_graphs <- paste("arima_graph", HEADER_NAME, 
                       sep = "-",
                       collapse = " ")
  name_pdf <- paste(name_graphs, "pdf", sep = ".")
  
  pdf(name_pdf)
  
  ## Preparando o dataframe
  dados_ts_na_removed <- na.approx(dados_ts)
  
  ## Retirando a parte final do dataframe que compromete o teste
  dados_ts_na_removed <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 144))
  
  ## Dividindo treino e teste
  dados_test <- tail(dados_ts_na_removed, n = FORECAST_WINDOW)
  
  dados_train <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - FORECAST_WINDOW))
  
  fit_power <- auto.arima(y = dados_train,
                          stepwise = FALSE,
                          approximation = FALSE,
                          seasonal = TRUE,
                          trace = FALSE,
                          D = 1)
  
  order_arima <- arimaorder(fit_power)
  
  write.table(HEADER_NAME, 
              file = "arima_order.txt",
              append = TRUE)
  write.table(order_arima,
              file = "arima_order.txt",
              append = TRUE)
  write.table("======",
              file = "arima_order.txt",
              append = TRUE)
  
  arima_forecast <- forecast(object = fit_power,
                             h = FORECAST_WINDOW,
                             level = 95)
  
  ## Previsão
  plot(arima_forecast, xlab = "Dias", 
       ylab = "Valores reais/previstos (Wh)", 
       main = "",
       ylim = c(-20000, 20000))
  lines(dados_ts_na_removed, lwd = 2, col = 'green')
  legend("bottomleft", c("Real", "Previsto"), lwd = c(1, 2), 
         col = c("green", "blue"), bty = 'o')
  
  dev.off()
  
  write.table(HEADER_NAME, 
              file = "arima_forecast.txt",
              append = TRUE)
  write.table(arima_forecast,
              file = "arima_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "arima_forecast.txt",
              append = TRUE)
  
  ## Métricas
  indices <- accuracy(arima_forecast, x = dados_test)
  
  write.table(HEADER_NAME, 
              file = "indices_arima.txt",
              append = TRUE)
  write.table(indices,
              file = "indices_arima.txt",
              append = TRUE)
  write.table("======",
              file = "indices_arima.txt",
              append = TRUE)
  
  
}

#####################################
#####################################
#####################################

run_arima_df <- function(df) {
  ## Libs necessarias
  require(anytime)
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(normtest)
  
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

  dados_ts <- ts(coluna_diferenca, frequency=24)
  
  ## Arquivo do gráfico
  name_graphs <- paste("arima_graph", df$house_id[1], 
                       sep = "-",
                       collapse = " ")
  name_pdf <- paste(name_graphs, "pdf", sep = ".")
  
  pdf(name_pdf)
  
  ## Preparando o dataframe
  dados_ts_na_removed <- na.approx(dados_ts)
  
  ## Retirando a parte final do dataframe que compromete o teste
  dados_ts_na_removed <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 144))
  
  ## Dividindo treino e teste
  dados_test <- tail(dados_ts_na_removed, n = FORECAST_WINDOW)
  
  dados_train <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - FORECAST_WINDOW))
  
  fit_power <- auto.arima(y = dados_train,
                          stepwise = FALSE,
                          approximation = FALSE,
                          seasonal = TRUE,
                          trace = FALSE,
                          D = 1)
  
  order_arima <- arimaorder(fit_power)
  
  write.table(HEADER_NAME, 
              file = "arima_order.txt",
              append = TRUE)
  write.table(order_arima,
              file = "arima_order.txt",
              append = TRUE)
  write.table("======",
              file = "arima_order.txt",
              append = TRUE)
  
  arima_forecast <- forecast(object = fit_power,
                             h = FORECAST_WINDOW,
                             level = c(90, 95))
  
  ## Previsão
  plot(arima_forecast, xlab = "Dias", ylab = "Valores reais/previstos (Wh)", main = "")
  lines(dados_ts_na_removed, lwd = 2, col = 'green')
  legend("bottomleft", c("Real", "Previsto"), lwd = c(1, 2), 
         col = c("green", "blue"), bty = 'o')
  
  dev.off()
  
  write.table(HEADER_NAME, 
              file = "arima_forecast.txt",
              append = TRUE)
  write.table(arima_forecast,
              file = "arima_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "arima_forecast.txt",
              append = TRUE)
  
  ## Métricas
  indices <- accuracy(arima_forecast, x = dados_test)
  
  write.table(HEADER_NAME, 
              file = "indices_arima.txt",
              append = TRUE)
  write.table(indices,
              file = "indices_arima.txt",
              append = TRUE)
  write.table("======",
              file = "indices_arima.txt",
              append = TRUE)
  
}