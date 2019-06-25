library(sparklyr)
library(dplyr)
library(dbplyr)
library(tidyverse)
library(lubridate)
library(tseries)
library(urca)

## Funcoes ARIMA e Holt-Winters para o sistema
source("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/arima-implementation.R")
source("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/holt-winters-implementation.R")

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

# df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted.csv", header = FALSE)
df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/filteredsorted.csv", header = FALSE)

names(df) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

end_time <- Sys.time()
end_time

end_time - start_time

##################################################################
### Filtragem por residencia e calculo do consumo horario
##################################################################

start_time_calculo_consumo_por_casa <- Sys.time()
start_time_calculo_consumo_por_casa

## Inicia o SparkR
sparkR.session(sparkHome = "/opt/spark")

for (house in 0:39) {
  df_house <- filter(df, house_id == house)

  ## Filtra o consumo acumulado
  df_house_present_consumption <- df_house %>% filter(work_or_load == 1)

  ## Remove a coluna de ID do CSV original
  df_house_present_consumption <- select(df_house_present_consumption, -id)

  ## Formata o timestamp em data e hora
  df_house_present_consumption <- df_house_present_consumption %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))

  ## Cria uma coluna com a media do consumo de cada plug por comodo
  df_house_hourly_mean_consumption <- df_house_present_consumption %>% group_by(hora, house_id, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))

  ## Soma o consumo de todos os plugs por hora
  df_house_hourly_consumption <- df_house_hourly_mean_consumption %>% group_by(hora, house_id) %>% arrange(hora) %>% summarise(total = sum(consumo))

  ##################################################################
  ### Aplicacao dos metodos de previsao e indices de acuracia
  ##################################################################

  ## Coletar o dataframe inteiro
  df_r <- sparklyr::collect(df_house_hourly_consumption)

  ## Cria a lista para adicionar os dataframes
  list_df <- list()
  list_df[[1]] <- df_r

  ## Aplica os métodos
  spark.lapply(list_df, run_arima_df)
  spark.lapply(list_df, run_hw_df)
}
# 
# end_time_calculo_consumo_por_casa <- Sys.time()
# end_time_calculo_consumo_por_casa
# 
# end_time_calculo_consumo_por_casa - start_time_calculo_consumo_por_casa

##############################################################
### Consumo consolidado de todas as residencias
##############################################################

start_time_calculo_consumo_all <- Sys.time()
start_time_calculo_consumo_all

## Filtra o consumo acumulado
df_all_present_consumption <- df %>% filter(work_or_load == 1)

## Remove a coluna de ID do CSV original
df_all_present_consumption <- select(df_all_present_consumption, -id)

## Formata o timestamp em data e hora
df_all_present_consumption <- df_all_present_consumption %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))

## Cria uma coluna com a media do consumo de cada plug por comodo
df_all_hourly_mean_consumption <- df_all_present_consumption %>% group_by(hora, house_id, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))

## Soma o consumo de todos os plugs por hora
df_all_hourly_consumption <- df_all_hourly_mean_consumption %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))

## Coletar o dataframe inteiro
# df_r_all <- sparklyr::collect(df_all_hourly_consumption)
df_r_all <- pull(df_all_hourly_consumption, total)

## Cria a lista para adicionar os dataframes
# list_df_all <- list()
# list_df_all[[1]] <- df_r_all

## Inicia o SparkR
# sparkR.session(sparkHome = "/opt/spark")

## Aplica os métodos
# spark.lapply(list_df_all, run_arima)
# spark.lapply(list_df_all, run_hw)

# Teste de estacionariedade
# stationarity_test <- kpss.test(ts(df_r_all))
stationarity_test_1 <- ur.kpss(ts(df_r_all), type = "tal", lags = "short")
stationarity_test_2 <- ur.kpss(ts(df_r_all), type = "mu", lags = "short")

stationarity_test_1
stationarity_test_2

base::summary(stationarity_test_1)
base::summary(stationarity_test_2)

end_time_calculo_consumo_all <- Sys.time()
end_time_calculo_consumo_all

end_time_calculo_consumo_all - start_time_calculo_consumo_all

# ##################################################################
# ### Filtragem por residencia - Calculo do consumo - Escrita CSVs - NAO UTILIZAR
# ##################################################################
# 
# start_time_calculo_consumo <- Sys.time()
# start_time_calculo_consumo
# 
# sparkR.session(sparkHome = "/opt/spark")
# 
# for (house in 0:39) {
#   df_house <- filter(df, house_id == house)
#   
#   ## Filtra o consumo acumulado
#   df_house_work <- df_house %>% filter(work_or_load == 0)
#   
#   ## Remove a coluna de ID do CSV original
#   df_house_work <- select(df_house_work, -id)
#   
#   ## Formata o timestamp em data e hora
#   df_house_work_datetime <- df_house_work %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))
#   
#   ## Cria uma coluna com o consumo acumulado de cada plug por comodo
#   df_house_cum <- df_house_work_datetime %>% group_by(hora, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = max(value))
#   
#   ## Soma o consumo de todos os plugs por hora
#   df_house_cum_hourly <- df_house_cum %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))
#   
#   ## Faz a diferenca de consumo entre a hora presente e a anterior
#   df_house_cum_hourly <- df_house_cum_hourly %>% select(hora, total) %>% mutate(base_value = first(total))
#   df_house_spent <- df_house_cum_hourly %>% group_by(hora) %>% arrange(hora) %>% mutate(diferenca = total - lag(total, default = base_value))
#   df_house_spent <- df_house_spent %>% select(-base_value)
#   
#   ## Extrair a coluna do consumo por hora
#   column_difference <- dplyr::pull(df_house_spent, diferenca)
#   
#   ## Criar uma lista para usar o metodo lapply e
#   ### inserir a coluna extraida
#   list_df <- list()
#   list_df[[1]] <- column_difference
#   
#   ## ARIMA
#   spark.lapply(list_df, run_arima)
#   
#   ## Holt-Winters
#   spark.lapply(list_df, run_hw)
# }
# 
# end_time_calculo_consumo <- Sys.time()
# end_time_calculo_consumo
# 
# end_time_calculo_consumo - start_time_calculo_consumo


##################################################################
### Calculo do consumo acumulado por hora
##################################################################

df_test <- spark_read_csv(sc, "file:///home/d3jota/debs/teste_out.csv", header = FALSE)

## Renomeia as colunas
names(df_test) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

## Filtra o consumo acumulado
df_test <- df_test %>% filter(work_or_load == 1)

## Remove a coluna de ID do CSV original
df_test <- select(df_test, -id)

## Formata o timestamp em data e hora
df_test3 <- df_test %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))

## Cria uma coluna com a media do consumo de cada plug por comodo
df_test4 <- df_test3 %>% group_by(hora, house_id, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))

## Soma o consumo de todos os plugs por hora
df_test5 <- df_test4 %>% group_by(hora, house_id) %>% arrange(hora) %>% summarise(total = sum(consumo))
# # 
# # ## Escrita de CSV
# # df_one <- sdf_repartition(df_test5, 1)
# # sparklyr::spark_write_csv(df_one, "/home/d3jota/teste/house_51", header = TRUE, delimiter = ",",
# #                           charset = "UTF-8", null_value = NULL,
# #                           options = list(), mode = "overwrite", partition_by = NULL)
# # 
## Extrair a coluna do consumo por hora
# column_difference <- dplyr::pull(df_test5, total)
# column_difference_with_index <- append(column_difference, 0)
# Coletar o dataframe inteiro para pegar o id da casa dentro da função!!! <<<<<<<<<<<<<<<<<<<<<<<<
df_r <- sparklyr::collect(df_test5)
# 
## Criar uma lista para usar o metodo lapply e
### inserir a coluna extraida
# list_df <- list()
# list_df[[1]] <- column_difference_with_index
# 
sparkR.session(sparkHome = "/opt/spark")

list_test <- list()
list_test[[1]] <- df_r

spark.lapply(list_test, run_arima_df)
spark.lapply(list_test, run_hw_df)
