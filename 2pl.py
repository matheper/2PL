#-------------------------------------------------------------------------------
# Nome:        2PL
# Autores:     Matheus Pereira
#
# Criado:      22/10/2012
#-------------------------------------------------------------------------------

import sys
import os.path

class Analisador():

    def __init__(self, arquivo):
        self.token = ''
        self.pos = 0
        self.arquivo = arquivo
        self.linhaArquivo = ''
        self.operacoes = []
        self.bloqueios = []
        self.historia = []
        self.delay = []

    def reconhecedor(self,linha):
        estado = 0
        posAnterior = 0
        tokenCorrente = ''
        while True:
            if estado == 0:
                if linha[self.pos] == ' ':
                    pass
                elif linha[self.pos] == 'r':
                    estado = 'READ'
                elif linha[self.pos] == 'w':
                    estado = 'WRITE'
                elif linha[self.pos] == 'c':
                    estado = 'COMMIT'
                elif linha[self.pos] >= 'a' and linha[self.pos] <= 'z':
                    estado = 'ID'
                elif linha[self.pos] == '(':
                    estado = 'OPEN'
                elif linha[self.pos] == ')':
                    estado = 'CLOSE'
                else:
                    return (0,0)
                if not linha[self.pos] in [' ','r','w','c'] :
                    self.token += linha[self.pos]
                self.pos += 1
            elif estado == 'OPEN' or estado == 'CLOSE':
                tokenCorrente = self.token
                self.token = ''
                return (estado, tokenCorrente)
            elif estado != 'ID':
                if linha[self.pos] >= '0' and linha[self.pos] <= '9':
                    self.token += linha[self.pos]
                    self.pos += 1
                else:
                    tokenCorrente = self.token
                    self.token = ''
                    return (estado, tokenCorrente)
            else:
                if linha[self.pos] >= 'a' and linha[self.pos] <= 'z':
                    self.token += linha[self.pos]
                    self.pos += 1
                else:
                    tokenCorrente = self.token
                    self.token = ''
                    return (estado,tokenCorrente)

    def analisadorLexico(self):
        if self.pos >= len(self.linhaArquivo) or self.linhaArquivo[self.pos] == '\n':
            self.linhaArquivo = self.arquivo.readline()
            self.pos = 0
        if self.pos < len(self.linhaArquivo):
            codToken = self.reconhecedor(self.linhaArquivo)
            return codToken
        return 0

    def analisadorSintatico(self):
        codToken = self.analisadorLexico()
        if not codToken:
            return True
        operacao = []
        if codToken[0] == 'READ' or codToken[0] == 'WRITE':
            operacao += codToken
            codToken = self.analisadorLexico()
            if codToken[0] == 'OPEN':
                codToken = self.analisadorLexico()
                if codToken[0] == 'ID':
                    operacao += codToken[1]
                    self.operacoes.append(operacao)
                    codToken = self.analisadorLexico()
                    if codToken[0] == 'CLOSE':
                        self.analisadorSintatico()
                    else:
                        print "Esperava fecha parentes."
                        sys.exit(0)
                else:
                    print "Espera dado para operacao."
                    sys.exit(0)
            else:
                print "Esperava abre parenteses."
                sys.exit(0)
        elif codToken[0] == 'COMMIT':
            operacao += codToken
            operacao.append('')
            self.operacoes.append(operacao)
            codToken = self.analisadorLexico()
            if codToken[0] == 'OPEN':
                codToken = self.analisadorLexico()
                if codToken[0] == 'CLOSE':
                    self.analisadorSintatico()
                else:
                    print "Espera fecha parenteses."
                    sys.exit(0)
            else:
                print "Esperava abre parenteses."
                sys.exit(0)
        else:
            print "Esperava operacao 'Read', 'Write', 'Commit'."
            sys.exit(0)

    def executarHistoria(self):
        self.analisadorSintatico()
        indiceOperacoes = 0
        indiceDelay = 0
        while self.operacoes or self.delay:
            print "Historia"
            print self.historia
            print "Operacoes"
            print self.operacoes
            print "Delay"
            print self.delay
            print "Bloqueios"
            print self.bloqueios
            print "\n\n\n"
            if indiceDelay >= len(self.delay):
                indiceDelay = 0
            if self.operacoes:
                operacao = self.operacoes[indiceOperacoes]
                if operacao[0] == 'COMMIT':
#                    import ipdb;ipdb.set_trace()
                    if operacao[1] in [lista[1] for lista in self.delay]:
                        self.delay.append(operacao)
                    else:
                        self.commitar(operacao)
                        self.historia.append(operacao)
                else:
                    if self.bloquear(operacao):
                        self.historia.append(operacao)
                    else:
                        self.delay.append(operacao)
                self.operacoes.remove(operacao)
            if self.delay:
                operacao = self.delay[indiceDelay]
                if operacao[0] == 'COMMIT':
                    if [lista[1] for lista in self.delay].count(operacao[1]) > 1:
                        indiceDelay += 1
                    else:
                        self.commitar(operacao)
                        self.historia.append(operacao)
                        self.delay.remove(operacao)
                else:
                    if self.bloquear(operacao):
                        self.historia.append(operacao)
                        self.delay.remove(operacao)
                    else:
                        indiceDelay += 1
            self.deadlock()

    def bloquear(self, operacao):
        if operacao[0] == 'READ':
            for bloqueio in self.bloqueios:
                if bloqueio[2] == operacao[2] and bloqueio[0] == 'WRITE':
                    if bloqueio[1] != operacao[1]:
                        return False
                    else:
                        return True
            if not operacao in self.bloqueios:
                self.bloqueios.append(operacao)
            return True
        if operacao[0] == 'WRITE':
            for bloqueio in self.bloqueios:
                if bloqueio[2] == operacao[2] and bloqueio[1] != operacao[1]:
                    return False
            if not operacao in self.bloqueios:
                self.bloqueios.append(operacao)
            return True

    def commitar(self, operacao):
        self.bloqueios[:] = filter(lambda x: x[1]!=operacao[1], self.bloqueios)

    def deadlock(self):
        grafo = {}
        for operacao in self.delay:
            for bloqueio in self.bloqueios:
                if operacao[2] == bloqueio[2] and operacao[1] != bloqueio[1]:
                    grafo[operacao[1]] = bloqueio[1]
        for vertice in grafo:
            try:
                if vertice in [ grafo[lista] for lista in grafo[vertice]]:
                    self.abortarOperacao(vertice)
            except:
                pass

    def abortarOperacao(self, transacao):
        listaOperacoes = []
        listaOperacoes[:] = filter(lambda x: x[1]==transacao, self.historia)  # Transfere operacoes da
        self.historia[:] = filter(lambda x: x[1]!=transacao, self.historia)   # historia para listaOperacoes,
        listaOperacoes[:] += filter(lambda x: x[1]==transacao, self.delay)    # o mesmo com o delay e com as operacoes.
        self.delay[:] = filter(lambda x: x[1]!=transacao, self.delay)         # Sempre nessa ordem para preservar a integridada
        listaOperacoes[:] += filter(lambda x: x[1]==transacao, self.operacoes)# da transacao.
        self.operacoes[:] = filter(lambda x: x[1]!=transacao, self.operacoes) #
        self.delay += listaOperacoes
        self.bloqueios[:] = filter(lambda x: x[0]!=transacao, self.bloqueios)

def main():
#   nomeArquivo = raw_input('Digite o nome do arquivo: ')
    nomeArquivo = 'Historia2pl.txt'
    if not os.path.exists(nomeArquivo):
        print 'Erro no carregamento do arquivo de inicializacao'
        return 0

    arquivo = open(nomeArquivo,'r')
    analisador = Analisador(arquivo)
    analisador.executarHistoria()
    print 'Historia:'
    print analisador.historia

    return 0

if __name__ == "__main__":
    main()
