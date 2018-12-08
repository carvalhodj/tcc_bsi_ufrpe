setwd("DEBS/")

media_por_casa <- function(arquivo_csv, nome_diretorio) {
    # Lendo o CSV base, desconsiderando os titulo das colunas, pois nao ha
    dados_lidos <- read.csv(arquivo_csv, header=FALSE)
    # Coletando os valores presentes na coluna de vizinhanca sem duplicidade, retornando um vetor
    household_ids <- unique(dados_lidos$V3)
    # Iterando entre as vizinhancas
    for (i in household_ids) {
        # Gerando um subdataframe a partir do dataframe original com apenas as casas da respectiva vizinhanca
        dados_por_household <- dados_lidos[which(dados_lidos$V3 == i), ]
        # Coletando os ids das casas da vizinhanca sem duplicidade, retornando um vetor
        houses_ids <- unique(dados_por_household$V4)
        # Iterando entre as casas da vizinhanca
        for (j in houses_ids) {
            # Gerando um subdataframe a partir do subdataframe gerado para a vizinhanca
            dados_house_household <- dados_por_household[which(dados_por_household$V4 == j), ]
            # Calculando a media de consumo da casa
            media <- mean(dados_house_household$V2)
            # Criando uma coluna de nome 'Media', armazenando o valor da media
            dados_house_household$Media <- media
            # Criando um diretorio para armazenar o subdataframe da casa
            dir.create(nome_diretorio, showWarnings=FALSE)
            # Criando um nome para o arquivo csv a ser gerado, contendo o id da vizinhanca seguido pelo id da casa
            name <- paste(nome_diretorio, "/", "dados_", i, "_", j, sep = "")
            # Escrevendo no arquivo csv os dados
            write.csv(dados_house_household, name)
        }
    }
}

media_por_casa("sorted_filtered_50mi.csv", "casa_por_vizinhanca")