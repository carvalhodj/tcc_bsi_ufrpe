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

## Configuracao e conexao com o Spark
config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.executor.memoryOverhead` <- "1g"

config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "/home/d3jota/.tmp")

sc <- sparklyr::spark_connect(master = "local", 
                              spark_home = "/opt/spark",
                              config = config)

## Contagem do tempo - inicio
start_time <- Sys.time()
start_time


setwd("/home/d3jota/combine/escrever_2/")

## Coleta o nome dos arquivos no diretorio presente
filenames <- list.files(full.names=TRUE)

## Preenche uma lista de dataframes com os csvs do diretorio
All <- lapply(filenames ,function(i){
  if (i != "./_SUCCESS") {
    read.csv(i, header=FALSE, skip=4)
  }
})

## Concatena todos os csvs da lista 'All' em um
df <- do.call(rbind.data.frame, All)

write.csv(df, "all_postcodes.csv", row.names=FALSE)

#######################
## PRIMEIRA PARTE
#######################

func_teste <- function(df) {
  df$tempo <- anytime::anytime(df$V2)
  df$datetime <- as.POSIXct(as.numeric(as.character(df$V2)),origin="1970-01-01",tz="GMT")
  df$Datetime2 <- droplevels(cut(df$datetime, breaks="hours"))
  agregado = aggregate(V3 ~ Datetime2, data=df, FUN=function(x) x[length(x)]-x[1])
}

## Remover outliers
outlierReplace = function(dataframe, cols, rows, newValue = NA) {
  if (any(rows)) {
    set(dataframe, rows, cols, newValue)
  }
}

func_teste2 <- function(df) {
  
  ## Substituir os outliers por NA
  outlierReplace(df, "V3", 
                 which(df$V3 > 1), NA)
  
  ## Substituir os NAs pela media do valor anterior e posterior
  df$V3 <- na.approx(df$V3)
  
  ## Criando a serie temporal com os dados lidos do csv
  ## Frequencia de 24 pois sao dados diarios medidos de hora em hora
  dados_ts <- ts(dados$V3, frequency=24)
}

run_holtwinters <- function(df) {
  ## Dataframe de teste
  dados.test.hw <- tail(df, n = 100)
  
  ## Dataframe de treino
  dados.train.hw <- head(df, n = (length(df) - 100))
  
  ajuste.holt <- HoltWinters(dados.train.hw)
  plot(df, xlab = 'tempo', ylab = 'Valores Observados/Ajustados', main = '')
  lines(fitted(ajuste.holt)[,1], lwd = 2, col = 'red')
  legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("black", "red"), bty = 'n')
  
  ## Previsao usando o Holt-Winters
  holt.forecast <- forecast(ajuste.holt, h = 100, level = 95)
  #plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  
  ## Plotagem comparando o treino com o teste
  plot(holt.forecast, xlab = "tempo", ylab = "Valores observados/previstos", main = "")
  lines(df, lwd = 2, col = 'green')
  legend(0, 20, c("Consumo", "Ajuste"), lwd = c(1, 2), col = c("green", "blue"), bty = 'n')
  
  ## Realizando uma previsão com método escolhido automagicamente e uma janela de 10 previsões
  dados.fore <- forecast(df, h=10)
  dados.fore
  
  ## gráfico de autocorrelação
  plot(acf(df))
}

## Leitura do csv
dados <- spark_read_csv(sc, "file:///home/d3jota/combine/escrever_1/all_postcodes.csv", header=TRUE)

## Convertendo o timestamp para data
dados4 <- dados %>% mutate(date = to_date((to_timestamp(V2)), "yyyy-MM-dd HH:mm"))
dados2 <- dados %>% mutate(datetime = (to_timestamp(V2)))

## Selecionar todas as colunas exceto a V1
dados <- select(dados, -V1)

## Filtrando o modo 'work', plug e comodo, respectivamente
dados2 <- dados %>% filter(V4 == 0)
dados2 <- dados2 %>% filter(V5 == 0)
dados2 <- dados2 %>% filter(V6 == 0)

## Escrevendo um CSV
sparklyr::spark_write_csv(dados2, "dados2.csv", header = FALSE, delimiter = ",",
                          charset = "UTF-8", null_value = NULL,
                          options = list(), mode = "overwrite", partition_by = NULL)


#######################
## SEGUNDA PARTE
#######################

setwd("/home/d3jota/x.csv/")
dados <- spark_read_csv(sc, "condensed.csv")
dados$datetime <- as.POSIXct(as.numeric(as.character(dados$V2)),origin="1970-01-01",tz="GMT")
dados$Datetime2 <- droplevels(cut(dados$datetime, breaks="hours"))
agregado = aggregate(V3 ~ Datetime2, data=dados, FUN=function(x) x[length(x)]-x[1])
# write.csv(agregado, "debs_consumo_agregado.csv")
# agregado
agr2 <- aggregate(dados$V3, by=list(dados$Datetime2), sum)

end_time <- Sys.time()
end_time

end_time - start_time


#dados3 <- mutate(hour_of_day = hour(as.POSIXct(strptime(datetime, "%Y-%m-%d %H:%M:%S")))) %>% group_by(hour_of_day) %>% summarise(soma = sum(V3))

## A diferenca com o anterior
#dados3 <- dados2 %>% group_by(datetime) %>% arrange(timestamp) %>% mutate(diff = V3 - lag(V3))

## Acumulado pelo group_by
#dados3 <- dados4 %>% group_by(date) %>% summarise(Y = sum(V3))

## Aplicar função
# teste <- dados2 %>% spark_apply(func_teste)