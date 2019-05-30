# dados <- read.csv("/home/d3jota/teste/house_51/house_51.csv")
# 
# plot(dados$diferenca)
# 
# dados$diferenca <- na.approx(dados$diferenca)

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
  
  FORECAST_WINDOW <- 72
  
  ## Eliminar outliers
  qnt <- quantile(column_difference, probs=c(.25, .75), na.rm = T)
  caps <- quantile(column_difference, probs=c(.05, .95), na.rm = T)
  H <- 1.5 * IQR(column_difference, na.rm = T)
  column_difference[column_difference < (qnt[1] - H)] <- caps[1]
  column_difference[column_difference > (qnt[2] + H)] <- caps[2]
  
  # qnt <- quantile(in_column_difference, probs=c(.25, .75), na.rm = TRUE)
  # # caps <- quantile(coluna_diferenca, probs=c(.05, .95), na.rm = T)
  # H <- 1.5 * IQR(in_column_difference, na.rm = TRUE)
  # column_difference <- in_column_difference
  # column_difference[in_column_difference < (qnt[1] - H)] <- NA
  # column_difference[in_column_difference > (qnt[2] + H)] <- NA
  # na.approx(column_difference)
  
  dados_ts <- ts(column_difference, frequency=24)
  
  ## Arquivo do gráfico
  name <- paste("arima_graphs", Sys.time(), sep = "_")
  name_pdf <- paste(name, "pdf", sep = ".")
  
  pdf(name_pdf)
  
  # #ts.plot(dados_ts, ylab ="Consumo de energia elétrica", xlab = "dias")
  # 
  # # plot(decompose(dados_ts))
  # 
  # ## Testar a estacionariedade das partes sazonal e não sazonal
  # ## Se a série temporal possuir uma Raiz Unitária (RU) então ela
  # ### não é estacionária
  # ## Teste de Dickey-Fuller aumentado (ADF)
  # 
  # adf_drift <- ur.df(y = diff(dados_ts), type = c("drift"), lags = 24,
  #                    selectlags = "AIC")
  # adf_drift
  # ## O valor de tau2 deve ser menor que o apresentado
  # ### em cval 5pct
  # adf_drift@teststat
  # adf_drift@cval
  # acf(diff(dados_ts))
  
  ## Estimar o modelo
  
  dados_ts <- na.approx(dados_ts)
  
  ## Dividindo treino e teste
  dados_test_hw <- tail(dados_ts, n = 2 * FORECAST_WINDOW)
  dados_test_hw <- head(dados_test_hw, n = FORECAST_WINDOW)
  
  dados_train <- head(dados_ts, n = (length(dados_ts) - (2 * FORECAST_WINDOW)))
  
  fit_power <- auto.arima(y = dados_train,
                          stepwise = FALSE,
                          approximation = FALSE,
                          seasonal = TRUE,
                          trace = TRUE,
                          D = 1)
  
  order_arima <- arimaorder(fit_power)
  
  write.table(order_arima,
              file = "arima_order.txt",
              append = TRUE)
  write.table("======",
              file = "arima_order.txt",
              append = TRUE)
  
  ## Box-Ljung
  ## Teste da ausência de autocorrelação linear
  # test_box <- Box.test(x = fit_power$residuals,
  #                      lag = 24,
  #                      type = "Ljung-Box",
  #                      fitdf = 2)
  # 
  ## Teste para confirmar a ausência de autocorrelação
  ### linear
  # test_box$p.value < test_box$statistic
  # test_box
  
  # ## Teste da ausência de autocorrelação da variância
  # require(FinTS)
  # test_arc <- ArchTest(fit_power$residuals,
  #                      lags = 12)
  
  ## Teste para confirmar a ausência de autocorrelação
  ### da variância
  # test_arc$p.value < test_arc$statistic
  # test_arc
  
  # ## Teste da normalidade
  # test_norm <- jb.norm.test(fit_power$residuals,
  #                           nrepl = 2000)
  
  ## Teste para confirmar a normalidade
  # test_norm$p.value < test_norm$statistic
  # test_norm
  
  arima_forecast <- forecast(object = fit_power,
                             h = FORECAST_WINDOW,
                             level = c(90, 95))
  
  ## Previsão
  plot(arima_forecast, xlab = "Dias", ylab = "Valores reais/previstos (Wh)", main = "")
  lines(dados_ts, lwd = 2, col = 'green')
  legend("topright", c("Real", "Previsto"), lwd = c(1, 2), 
         col = c("green", "blue"), bty = 'o')
  
  write.table(arima_forecast,
              file = "arima_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "arima_forecast.txt",
              append = TRUE)
  
  ## Métricas
  indices <- accuracy(arima_forecast, x = dados_test)
  
  indices_season <- accuracy(arima_forecast, x = dados_test, d = 0, D = 1)
  
  write.table(indices,
              file = "indices_arima.txt",
              append = TRUE)
  write.table("======",
              file = "indices_arima.txt",
              append = TRUE)
  
  write.table(indices_season,
              file = "indices_season_arima.txt",
              append = TRUE)
  write.table("======",
              file = "indices_season_arima.txt",
              append = TRUE)
  
  dev.off()
  
}