library(anytime)
library(forecast)
library(urca)
library(tseries)
library(MLmetrics)
library(data.table)
library(zoo)
library(sparklyr)
library(dplyr)
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

## Filtra o consumo acumulado
df_test <- df_test %>% filter(V4 == 0)
# df_test <- df_test %>% filter(V5 == 0)
# df_test <- df_test %>% filter(V6 == 0)

## Remove a coluna de ID do CSV original
df_test <- select(df_test, -V1)

reduce

## Criando uma coluna com o acumulado geral em cada timestamp
df_test <- df_test %>% arrange(V2) %>% mutate(acumulado = cumsum(V3))

#df_test2 <- df_test %>% mutate(datatempo = (to_timestamp(V2)))

df_test3 <- df_test2 %>% mutate(hora = from_unixtime(V2, 'yyyy-MM-dd HH'))
df_test4 <- df_test3 %>% group_by(hora, V6, V5) %>% arrange(hora, V6, V5) %>% summarise(consumo = max(V3))
df_test5 <- df_test3 %>% group_by(hora, V6, V5) %>% arrange(hora, V6, V5) %>% summarise(consumo = max(V3) - lag(max(V3)))

df_test2 <- df_test %>% group_by(datatempo = cut(datatempo, breaks = "hours")) %>% 
  
func_teste <- function(df) {
  df$tempo <- anytime::anytime(df$V2)
  # df$datetime <- as.POSIXct(as.numeric(as.character(df$V2)),origin="1970-01-01",tz="GMT")
  # df$Datetime2 <- droplevels(cut(df$datetime, breaks="hours"))
  # agregado = aggregate(acumulado ~ Datetime2, data=df, FUN=function(x) x[length(x)]-x[1])
}

start_time_3 <- Sys.time()
start_time_3

teste <- df_test %>% spark_apply(func_teste)

end_time_3 <- Sys.time()
end_time_3

end_time_3 - start_time_3

head(teste)

df_test_2 <- df_test %>% mutate(datetime = (to_timestamp(V2)))
teste <- df_test_2 %>% group_by(datetime, V6, V5) %>% arrange(datetime) %>% mutate(diff = V3 - lag(V3))
teste2 <- teste %>% select(-V1)
teste3 <- teste2 %>% spark_apply(func_teste)
