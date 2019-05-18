valores = []
contador = 1

with open('indices_hw.txt') as f:
    for line in f:
        if len(line.split()) > 1 and line.split()[1] == "\"============================\"":
            filename = "house_{}.txt".format(contador)
            del valores[-1]
            with open(filename, 'w') as g:
                for value in valores:
                    g.write("{}\n".format(value))
            print("casa {}".format(contador))
            contador += 1
        else:
            valores.append(line)
#            print(line)
