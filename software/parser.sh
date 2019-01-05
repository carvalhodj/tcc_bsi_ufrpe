#!/bin/sh

# Parser para o arquivo csv DEBS 2014 coletando dados essenciais para mensurar o consumo de energia

# 1 = quantidade de linhas
# 2 = arquivo csv
# 3 = arquivo destino

head -n $1 $2 | grep 1,0,1,0$ | cut -d "," -f 2,3,4,5,6,7 > $3