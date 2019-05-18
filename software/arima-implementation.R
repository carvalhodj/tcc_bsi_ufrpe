# dados <- read.csv("/home/d3jota/teste/house_51/house_51.csv")
# 
# plot(dados$diferenca)
# 
# dados$diferenca <- na.approx(dados$diferenca)

run_arima <- function(column_difference) {
  ## Libs necessarias
  require(anytime)
  require(forecast)
  require(urca)
  require(tseries)
  require(MLmetrics)
  require(data.table)
  require(zoo)
  require(normtest)
  
  dados_ts <- ts(column_difference, frequency=24)
  
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
  
  dados_ts <- na.remove(dados_ts)
  
  ## Dividindo treino e teste
  dados_test <- tail(dados_ts, n = 72)
  dados_train <- head(dados_ts, n = (length(dados_ts) - 72))
  
  fit_power <- auto.arima(y = dados_train,
                          stepwise = FALSE,
                          approximation = FALSE,
                          seasonal = TRUE,
                          trace = TRUE)
  
  order_arima <- arimaorder(fit_power)
  
  write.table(order_arima,
              file = "arima_order.txt",
              append = TRUE)
  write.table("======",
              file = "arima_order.txt",
              append = TRUE)
  
  ## Box-Ljung
  ## Teste da ausência de autocorrelação linear
  test_box <- Box.test(x = fit_power$residuals,
                       lag = 24,
                       type = "Ljung-Box",
                       fitdf = 2)
  
  ## Teste para confirmar a ausência de autocorrelação
  ### linear
  # test_box$p.value < test_box$statistic
  # test_box
  
  ## Teste da ausência de autocorrelação da variância
  require(FinTS)
  test_arc <- ArchTest(fit_power$residuals,
                       lags = 12)
  
  ## Teste para confirmar a ausência de autocorrelação
  ### da variância
  # test_arc$p.value < test_arc$statistic
  # test_arc
  
  ## Teste da normalidade
  test_norm <- jb.norm.test(fit_power$residuals,
                            nrepl = 2000)
  
  ## Teste para confirmar a normalidade
  # test_norm$p.value < test_norm$statistic
  # test_norm
  
  ## Previsão
  # plot(forecast(object = fit_power,
  #               h = 100,
  #               level = 0.95))
  
  arima_forecast <- forecast(object = fit_power,
                             h = 72,
                             level = 0.95)
  
  write.table(arima_forecast,
              file = "arima_forecast.txt",
              append = TRUE)
  write.table("======",
              file = "arima_forecast.txt",
              append = TRUE)
  
  ## Métricas
  indices <- accuracy(arima_forecast, x = dados_test)
  
  write.table(indices,
              file = "indices_arima.txt",
              append = TRUE)
  write.table("======",
              file = "indices_arima.txt",
              append = TRUE)
  
  
  
}