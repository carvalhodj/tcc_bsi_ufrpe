library(sparklyr)
library(tidyverse)
# 1,2 horas

config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.executor.memoryOverhead` <- "1g"

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
