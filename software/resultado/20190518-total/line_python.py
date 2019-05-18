import sys

valores = []
contador = 1
NOME_ARQUIVO = sys.argv[1]
NOME_ARQUIVO_SPLIT = NOME_ARQUIVO.split('.')[0]

with open(NOME_ARQUIVO) as f:
    for line in f:
        if len(line.split()) > 1 and line.split()[1] == "\"============================\"":
            filename = "{}_house_{}.txt".format(NOME_ARQUIVO_SPLIT, contador)
            del valores[-1]
            with open(filename, 'w') as g:
                for value in valores:
                    g.write("{}\n".format(value))
            print("casa {}".format(contador))
            contador += 1
        else:
            valores.append(line)
#            print(line)
