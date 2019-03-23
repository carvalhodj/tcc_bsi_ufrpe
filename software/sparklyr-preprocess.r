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

# df_pattern = select(df, `03`, `11`, `05`, `06`)
# 
# df_pattern <- df_pattern %>% rename("type" = "03")
# df_pattern <- df_pattern %>% rename("plug" = "11")
# df_pattern <- df_pattern %>% rename("household" = "05")
# df_pattern <- df_pattern %>% rename("house" = "06")
# 
# df_write <- dplyr::distinct(df_pattern, type, plug, household, house)
# 
# df_houses_distinct <- dplyr::distinct(df_pattern, house)
# 
# ## Para unir os csvs em um so
# df_write <- sdf_coalesce(df_write,
#                          partitions = 1)
# 
# df_write <- sdf_coalesce(df_houses_distinct,
#                          partitions = 1)

lista <- list()

for (house in 1:10) {
  #print(house)
  
  df_loop <- filter(df, `06` == house)
  #df_loop <- sdf_coalesce(df_loop,
  #                        partitions = 1)
  lista[[house]] <- df_loop
}
## PROBLEMA AO REALIZAR A CONCATENACAO DOS ARQUIVOS CSV COM O COALESCE. POSSIVEL FALTA DE MEMORIA.
# df_to_write <- sdf_coalesce(lista[[2]],
#                             partitions = 1)
# sparklyr::spark_write_csv(lista[[2]], paste(c("combine/escrever", 2), collapse = "_"), header = TRUE, delimiter = ",",
#                           charset = "UTF-8", null_value = NULL,
#                           options = list(), mode = "overwrite", partition_by = NULL)


for (i in 1:10) {
  # df_to_write <- sdf_coalesce(lista[[i]],
  #                             partitions = 1)
  sparklyr::spark_write_csv(lista[[i]], paste(c("combine/escrever", i), collapse = "_"), header = FALSE, delimiter = ",",
                            charset = "UTF-8", null_value = NULL,
                            options = list(), mode = "overwrite", partition_by = NULL)
}

# df_to_write <- sdf_coalesce(lista[[1]],
#                             partitions = 1)
# sparklyr::spark_write_csv(df_to_write, paste(c("combine/escrever", 1), collapse = "_"), header = TRUE, delimiter = ",",
#                           charset = "UTF-8", null_value = NULL,
#                           options = list(), mode = "overwrite", partition_by = NULL)
# 
# ## Escrever o csv
# sparklyr::spark_write_csv(df_write, "combine/escrever", header = TRUE, delimiter = ",",
#                           charset = "UTF-8", null_value = NULL,
#                           options = list(), mode = "overwrite", partition_by = NULL)
