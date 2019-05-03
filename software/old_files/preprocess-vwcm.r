library(dplyr)
library(tidyverse)
library(lubridate)

########################################################
### calculo do consumo acumulado por hora
########################################################

df <- read.csv(file="./csv_files/part-00000-3df2ef7b-02e1-4c16-b696-aed730198012-c000.csv", header = FALSE)
names(df) <- c("id", "ts", "value","work_or_load","plug_id","household_id","house_id")

# filtra apenas o consumo acumulado
df <- df %>% filter(work_or_load == 0)

# remove a coluna de ID do CSV original
df <- select(df, -id)
# insere a coluna hora baseado no timestamp
df <- df %>% mutate(hora = floor_date(as_datetime(ts), unit = "hour"))
# agrupa todos os valores dentro de uma mesma hora e pega o valor maximo
df <- df %>% group_by(hora, household_id, plug_id) %>% summarise(consumo_acumulado = max(value))
# calcula o consumo por hora fazendo a diferenca de consumo de uma janela de uma hora para outra agrupando por comodo e tomada
df <- df %>% group_by(household_id, plug_id) %>% mutate(consumo_por_hora = consumo_acumulado - lag(consumo_acumulado, order_by = hora)) %>% arrange(household_id, plug_id)

write.csv(df, file = "df.csv")