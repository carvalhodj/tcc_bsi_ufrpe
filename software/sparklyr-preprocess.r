library(sparklyr)
library(tidyverse)
# 1,2 horas

config = spark_config()
config$`sparklyr.backend.threads` <- "4"
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "1g"

config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "/home/d3jota/.tmp")

sc <- sparklyr::spark_connect(master = "local", 
                              spark_home = "/opt/spark",
                              config = config)
start_time <- Sys.time()
start_time

# df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted-100kk.csv")
# df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/tcc_bsi_ufrpe/software/csv_files/filtered0010-consumo_agregado.csv")
df <- spark_read_csv(sc, "file:///home/d3jota/UFRPE/BSI/TCC/sorted.csv")

end_time <- Sys.time()
end_time

end_time - start_time

lista <- list()

for (house in 1:10) {
  df_loop <- filter(df, `06` == house)
  lista[[house]] <- df_loop
}

for (i in 1:10) {
  sparklyr::spark_write_csv(lista[[i]], paste(c("combine/escrever", i), collapse = "_"), header = FALSE, delimiter = ",",
                            charset = "UTF-8", null_value = NULL,
                            options = list(), mode = "overwrite", partition_by = NULL)
}