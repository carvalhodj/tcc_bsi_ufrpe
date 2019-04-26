library(anytime)
library(forecast)
library(urca)
library(tseries)
library(MLmetrics)
library(data.table)
library(zoo)
library(sparklyr)
library(dplyr)
library(dbplyr)
library(tidyverse)
library(lubridate)

# 1,2 horas

config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.executor.memoryOverhead` <- "1g"
# config$`spark.network.timeout` <- "700s"
# config$`spark.executor.heartbeatInterval` <- "600s"

config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "/home/d3jota/.tmp")

sc <- sparklyr::spark_connect(master = "local", 
                              spark_home = "/opt/spark",
                              config = config)

########################################################
### Leitura do CSV original
########################################################

start_time <- Sys.time()
start_time

df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted.csv", header = FALSE)

end_time <- Sys.time()
end_time

end_time - start_time

########################################################
### Filtragem por residencia e escrita dos CSVs
########################################################

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

########################################################
### Calculo do consumo acumulado por hora
########################################################

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
df_test6 <- df_test5 %>% group_by(hora) %>% arrange(hora) %>% mutate(diferenca = total - lag(total, default = 99.2))

## Escrita de CSV
df_one <- sdf_repartition(df_test6, 1)
sparklyr::spark_write_csv(df_one, "teste/house_5", header = TRUE, delimiter = ",",
                          charset = "UTF-8", null_value = NULL,
                          options = list(), mode = "overwrite", partition_by = NULL)
