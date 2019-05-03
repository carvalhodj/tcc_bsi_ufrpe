library(sparklyr)
library(dplyr)
library(dbplyr)
library(tidyverse)
library(lubridate)

##################################################################
### Iniciar conexao com o spark
##################################################################

config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.executor.memoryOverhead` <- "1g"

config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "/home/d3jota/.tmp")

sc <- sparklyr::spark_connect(master = "local", 
                              spark_home = "/opt/spark",
                              config = config)

##################################################################
### Leitura do CSV original
##################################################################

start_time <- Sys.time()
start_time

df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted.csv", header = FALSE)

names(df) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

end_time <- Sys.time()
end_time

                                                                                                                                                                                                                                                                            end_time - start_time

##################################################################
### Filtragem por residencia e escrita dos CSVs - NAO UTILIZAR
##################################################################

start_time_2 <- Sys.time()
start_time_2

lista <- list()

for (house in 0:39) {
  df_loop <- filter(df, V7 == house)
  lista[[house+1]] <- df_loop
}

for (i in 0:39) {
  df_condensed <- sdf_repartition(lista[[i+1]], 1)
  sparklyr::spark_write_csv(df_condensed, paste(c("debs/house", i), collapse = "_"), header = FALSE, delimiter = ",",
                            charset = "UTF-8", null_value = NULL,
                            options = list(), mode = "overwrite", partition_by = NULL)
}

end_time_2 <- Sys.time()
end_time_2

end_time_2 - start_time_2

##################################################################
### Filtragem por residencia - Calculo do consumo - Escrita CSVs - NAO UTILIZAR
##################################################################

start_time_calculo_consumo <- Sys.time()
start_time_calculo_consumo

for (house in 0:39) {
  df_house <- filter(df, house_id == house)
  
  ## Filtra o consumo acumulado
  df_house <- df_house %>% filter(work_or_load == 0)
  
  ## Remove a coluna de ID do CSV original
  df_house <- select(df_house, -id)
  
  ## Formata o timestamp em data e hora
  df_house <- df_house %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))
  
  ## Cria uma coluna com o consumo acumulado de cada plug por comodo
  df_house <- df_house %>% group_by(hora, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = max(value))
  
  ## Soma o consumo de todos os plugs por hora
  df_house <- df_house %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))
  
  ## Faz a diferenca de consumo entre a hora presente e a anterior
  df_house <- df_house %>% group_by(hora) %>% arrange(hora) %>% mutate(diferenca = total - lag(total, default = 99.2))
  
  ## Escrita de CSV
  df_one <- sdf_repartition(df_house, 1)
  sparklyr::spark_write_csv(df_one, paste(c("debs/consumo/house", i), collapse = "_"), header = TRUE, delimiter = ",",
                            charset = "UTF-8", null_value = NULL,
                            options = list(), mode = "overwrite", partition_by = NULL)
}

end_time_calculo_consumo <- Sys.time()
end_time_calculo_consumo

end_time_calculo_consumo - start_time_calculo_consumo

##################################################################
### Calculo do consumo acumulado por hora
##################################################################

df_test <- spark_read_csv(sc, "file:///home/d3jota/debs/house_5/part-00000-3df2ef7b-02e1-4c16-b696-aed730198012-c000.csv", header = FALSE)

## Renomeia as colunas
names(df_test) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

## Filtra o consumo acumulado
df_test <- df_test %>% filter(work_or_load == 0)

## Remove a coluna de ID do CSV original
df_test <- select(df_test, -id)

## Formata o timestamp em data e hora
df_test3 <- df_test %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))

## Cria uma coluna com o consumo acumulado de cada plug por comodo
df_test4 <- df_test3 %>% group_by(hora, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = max(value))

## Soma o consumo de todos os plugs por hora
df_test5 <- df_test4 %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))

## Faz a diferenca de consumo entre a hora presente e a anterior
df_test5 <- df_test5 %>% select(hora, total) %>% mutate(base_value = first(total))
df_test6 <- df_test5 %>% group_by(hora) %>% arrange(hora) %>% mutate(diferenca = total - lag(total, default = base_value))
df_test6 <- df_test6 %>% select(-base_value)

## Escrita de CSV
df_one <- sdf_repartition(df_test6, 1)
sparklyr::spark_write_csv(df_one, "/home/d3jota/teste/house_51", header = TRUE, delimiter = ",",
                          charset = "UTF-8", null_value = NULL,
                          options = list(), mode = "overwrite", partition_by = NULL)

##############################################################
### Metodos de previsao
##############################################################

## Rodar o HoltWinters

# sparkR.session(sparkHome = "/opt/spark")

# column_difference <- dplyr::pull(df_test6, diferenca)

# listao <- list()
# listao[[1]] <- column_difference

# spark.lapply(listao, run_holtwinters)

# run_holtwinters <- function(coluna_diferenca) {
#   ## Libs
#   require(forecast)
#   require(urca)
#   require(tseries)
#   require(MLmetrics)
#   require(data.table)
#   require(zoo)
#   require(stats)
#   
#   #coluna_diferenca <- sdf_read_column(df, "diferenca")
#   #coluna_diferenca <- df[, "diferenca"]
#   #head(coluna_diferenca)
#   #class(df)
#   ## Substituir os NAs pela media do valor anterior e posterior
#   #coluna_diferenca <- na.approx(coluna_diferenca)
#   
#   # ## Criando a timeseries
#   dados_ts <- ts(coluna_diferenca, frequency=24)
# 
# 
#   ## Verificando a estacionariedade
#   dados_teste_estacionariedade <- summary(ur.kpss(diff(diff(dados_ts))))
#   # dados_teste_estacionariedade
# 
#   # dados_ts <- diff(diff(dados_ts))
# 
#   ## Tratamento necessário para realizar o forecast
#   dados_ts_na_removed <- na.remove(dados_ts)
# 
#   ## Dataframe de teste
#   dados_test_hw <- tail(dados_ts_na_removed, n = 100)
# 
#   ## Dataframe de treino
#   dados_train_hw <- head(dados_ts_na_removed, n = (length(dados_ts_na_removed) - 100))
# 
#   ajuste_holt <- HoltWinters(dados_train_hw)
#   plot(dados_ts_na_removed, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
#   lines(fitted(ajuste_holt)[,1], lwd = 2, col = 'red')
#   legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')
# 
#   ## Previsao usando o Holt-Winters
#   holt_forecast <- forecast(ajuste_holt, h = 100, level = 95)
#   #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
# 
#   ## Plotagem comparando o treino com o teste
#   plot(holt_forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
#   lines(dados_ts_na_removed, lwd = 2, col = 'green')
#   legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
#   print(holt_forecast)
#   
#   ## Calculo dos indices de erro
#   accuracy(holt_forecast, x = dados_test_hw)
# }
