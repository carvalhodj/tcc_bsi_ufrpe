library(anytime)
library(forecast)
library(data.table)

setwd("DEBS/")

# TODO - função com erros, refazer
# media_por_casa <- function(arquivo_csv, nome_diretorio) {
#     # Lendo o CSV base, desconsiderando os titulo das colunas, pois nao ha
#     dados_lidos <- read.csv(arquivo_csv, header=FALSE)
#     # Coletando os valores presentes na coluna de vizinhanca sem duplicidade, retornando um vetor
#     household_ids <- unique(dados_lidos$V3)
#     # Iterando entre as vizinhancas
#     for (i in household_ids) {
#         # Gerando um subdataframe a partir do dataframe original com apenas as casas da respectiva vizinhanca
#         dados_por_household <- dados_lidos[which(dados_lidos$V3 == i), ]
#         # Coletando os ids das casas da vizinhanca sem duplicidade, retornando um vetor
#         houses_ids <- unique(dados_por_household$V4)
#         # Iterando entre as casas da vizinhanca
#         for (j in houses_ids) {
#             # Gerando um subdataframe a partir do subdataframe gerado para a vizinhanca
#             dados_house_household <- dados_por_household[which(dados_por_household$V4 == j), ]
#             # Calculando a media de consumo da casa
#             media <- mean(dados_house_household$V2)
#             # Criando uma coluna de nome 'Media', armazenando o valor da media
#             dados_house_household$Media <- media
#             # Criando um diretorio para armazenar o subdataframe da casa
#             dir.create(nome_diretorio, showWarnings=FALSE)
#             # Criando um nome para o arquivo csv a ser gerado, contendo o id da vizinhanca seguido pelo id da casa
#             name <- paste(nome_diretorio, "/", "dados_", i, "_", j, sep = "")
#             # Escrevendo no arquivo csv os dados
#             write.csv(dados_house_household, name)
#         }
#     }
# }

hrs <- function(u) {
 x <- u * 3600
 return(x)
}
 
mns <- function(m) {
 x <- m * 60
 return(x)
}

# PRIMEIRA PARTE
# dados <- read.csv("sorted_tempo.csv", header=FALSE)
# dados$tempo <- anytime(dados$V1)
# dados$tempo
# write.csv(dados, "sorted_tempo.csv")

dados <- read.csv("sorted_tempo.csv")
dt <- data.table(dados)
dt[ , Date:= as.Date( tempo ) ]
dt$Teste <- difftime(dt$tempo, shift(dt$tempo), units="secs")
head(dt)
#dt[ , Teste:= as.difftime( tempo, shift(tempo), units="secs" )]
##head(dt)
# Pega a última linha de cada dia
##x <- dt[ , .SD[.N] ,  by = c("Date") ]
# Calcula a diferença do acumulado entre o dia atual e o anterior
##x[ , diff:= V2 - shift(V2)]


#x
#y <- dt[1, shift(tempo)]
#y
#x1 <- dt[1, tempo]
#x2 <- dt[2, tempo]
#difftime(x2, x1, units="secs")