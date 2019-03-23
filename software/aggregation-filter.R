library(anytime)
library(forecast)
library(urca)
library(tseries)
library(MLmetrics)
library(data.table)
library(zoo)
library(sparklyr)
library(tidyverse)

## Configuracao e conexao com o Spark
config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "1g"

config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "/home/d3jota/.tmp")

sc <- sparklyr::spark_connect(master = "local", 
                              spark_home = "/opt/spark",
                              config = config)

## Contagem do tempo - inicio
start_time <- Sys.time()
start_time


setwd("/home/d3jota/combine/escrever_1/")

## Concatenacao dos arquivos csvs menores
filenames <- list.files(full.names=TRUE)

All <- lapply(filenames ,function(i){
  if (i != "./_SUCCESS") {
    read.csv(i, header=FALSE, skip=4)
  }
})

df <- do.call(rbind.data.frame, All)

write.csv(df, "all_postcodes.csv", row.names=FALSE)

## PRIMEIRA PARTE
# dados <- read.csv("/home/d3jota/combine/escrever_1/all_postcodes.csv", header=FALSE)
dados <- spark_read_csv(sc, "file:///home/d3jota/combine/escrever_1/all_postcodes.csv", header=FALSE)
# dados$tempo <- anytime(dados$V2)
# dados$tempo
dados$V1 <- NULL

dados2 <- dados[dados$V4 == 0, ]
dados2 <- dados2[dados2$V6 == 1, ]
dados2 <- dados2[dados2$V5 == 0, ]

sparklyr::spark_write_csv(dados2, "dados2.csv", header = FALSE, delimiter = ",",
                          charset = "UTF-8", null_value = NULL,
                          options = list(), mode = "overwrite", partition_by = NULL)

# head(dados2)
# write.csv(dados, "sorted_tempo.csv")
# ## SEGUNDA PARTE
dados <- spark_read_csv(sc, "dados2.csv")
dados$datetime <- as.POSIXct(as.numeric(as.character(dados$V2)),origin="1970-01-01",tz="GMT")
dados$Datetime2 <- droplevels(cut(dados$datetime, breaks="hours"))
agregado = aggregate(V3 ~ Datetime2, data=dados, FUN=function(x) x[length(x)]-x[1])
# write.csv(agregado, "debs_consumo_agregado.csv")
# agregado
agr2 <- aggregate(dados$V3, by=list(dados$Datetime2), sum)

end_time <- Sys.time()
end_time

end_time - start_time
