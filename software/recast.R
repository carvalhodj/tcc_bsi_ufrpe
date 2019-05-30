library(sparklyr)
library(dplyr)
library(dbplyr)
library(tidyverse)
library(lubridate)

## Funcoes ARIMA e Holt-Winters para o sistema
source("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/arima-implementation.R")
source("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/holt-winters-implementation.R")
source("/home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/utils.R")

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
### Filtragem por residencia e calculo do consumo horario
##################################################################

start_time_calculo_consumo <- Sys.time()
start_time_calculo_consumo

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
  df_house_hourly_mean_consumption <- df_house_present_consumption %>% group_by(hora, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))
  
  ## Soma o consumo de todos os plugs por hora
  df_house_hourly_consumption <- df_house_hourly_mean_consumption %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))
  
##################################################################
### Aplicacao dos metodos de previsao e indices de acuracia
##################################################################
  ## Extrair a coluna do consumo por hora
  column_difference <- dplyr::pull(df_house_hourly_consumption, total)
  
  ## Criar uma lista para usar o metodo lapply e
  ### inserir a coluna extraida
  list_df <- list()
  list_df[[1]] <- column_difference
  
  ## ARIMA
  spark.lapply(list_df, run_arima)
  
  ## Holt-Winters
  spark.lapply(list_df, run_hw)
}

end_time_calculo_consumo <- Sys.time()
end_time_calculo_consumo

end_time_calculo_consumo - start_time_calculo_consumo

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
### Filtragem por residencia e escrita dos CSVs - NAO UTILIZAR
##################################################################

# start_time_2 <- Sys.time()
# start_time_2
# 
# lista <- list()
# 
# for (house in 0:39) {
#   df_loop <- filter(df, V7 == house)
#   lista[[house+1]] <- df_loop
# }
# 
# for (i in 0:39) {
#   df_condensed <- sdf_repartition(lista[[i+1]], 1)
#   sparklyr::spark_write_csv(df_condensed, paste(c("debs/house", i), collapse = "_"), header = FALSE, delimiter = ",",
#                             charset = "UTF-8", null_value = NULL,
#                             options = list(), mode = "overwrite", partition_by = NULL)
# }
# 
# end_time_2 <- Sys.time()
# end_time_2
# 
# end_time_2 - start_time_2

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
df_test4 <- df_test3 %>% group_by(hora, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))

## Soma o consumo de todos os plugs por hora
df_test5 <- df_test4 %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))
# # 
# # ## Escrita de CSV
# # df_one <- sdf_repartition(df_test5, 1)
# # sparklyr::spark_write_csv(df_one, "/home/d3jota/teste/house_51", header = TRUE, delimiter = ",",
# #                           charset = "UTF-8", null_value = NULL,
# #                           options = list(), mode = "overwrite", partition_by = NULL)
# # 
## Extrair a coluna do consumo por hora
column_difference <- dplyr::pull(df_test5, total)

## Coletar o dataframe inteiro para pegar o id da casa dentro da função!!! <<<<<<<<<<<<<<<<<<<<<<<<
# df_r <- sparklyr::collect(df_test5)
# 
## Criar uma lista para usar o metodo lapply e
### inserir a coluna extraida
list_df <- list()
list_df[[1]] <- column_difference
# 
sparkR.session(sparkHome = "/opt/spark")

# list_test <- list()
# list_test[[1]] <- df_r

spark.lapply(list_df, run_arima)
spark.lapply(list_df, run_hw)

##############################################################
### Total
##############################################################

df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted.csv", header = FALSE)

names(df) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

## Filtra o consumo acumulado
df_house_present_consumption <- df %>% filter(work_or_load == 1)

## Remove a coluna de ID do CSV original
df_house_present_consumption <- select(df_house_present_consumption, -id)

## Formata o timestamp em data e hora
df_house_present_consumption <- df_house_present_consumption %>% mutate(hora = from_unixtime(ts, 'yyyy-MM-dd HH'))

## Cria uma coluna com a media do consumo de cada plug por comodo
df_house_hourly_mean_consumption <- df_house_present_consumption %>% group_by(hora, house_id, household_id, plug_id) %>% arrange(hora, household_id, plug_id) %>% summarise(consumo = mean(value))

## Soma o consumo de todos os plugs por hora
df_house_hourly_consumption <- df_house_hourly_mean_consumption %>% group_by(hora) %>% arrange(hora) %>% summarise(total = sum(consumo))

## Extrair a coluna do consumo por hora
column_difference <- dplyr::pull(df_house_hourly_consumption, total)

## Criar uma lista para usar o metodo lapply e
### inserir a coluna extraida
list_df <- list()
list_df[[1]] <- column_difference

sparkR.session(sparkHome = "/opt/spark")

## ARIMA
spark.lapply(list_df, run_arima)

## Holt-Winters
spark.lapply(list_df, run_hw)
