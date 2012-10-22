#-------------------------------------------------------------------------------
# Nome: 2PL
# Autores: Luiz Eduardo
# Matheus Pereira
#
# Criado: 13/10/2012
#-------------------------------------------------------------------------------

import sys
import os.path

token = ''
pos = 0
arquivo = None
linhaArquivo = ''

READ = 1
WRITE = 2
COMMIT = 3
ABORT = 4
ID = 5
OPEN = 6
CLOSE = 7

def reconhecedor(linha):
    global pos
    global token
    global pos
    global READ
    global WRITE
    global COMMIT
    global ABORT
    global ID
    global OPEN
    global CLOSE

    estado = 0
    posAnterior = 0
    tokenCorrente = ''
    while True:
        if estado == 0:
            if linha[pos] == ' ':
                pass
            elif linha[pos] == 'r':
                estado = READ
            elif linha[pos] == 'w':
                estado = WRITE
            elif linha[pos] == 'c':
                estado = COMMIT
            elif linha[pos] == 'a':
                estado = ABORT
            elif linha[pos] >= 'a' and linha[pos] <= 'z':
                estado = ID
            elif linha[pos] == '(':
                estado = OPEN
            elif linha[pos] == ')':
                estado = CLOSE
            else:
                return (0,0)
            if linha[pos] != ' ':
                token += linha[pos]
            pos += 1
        elif estado == OPEN or estado == CLOSE:
            tokenCorrente = token
            token = ''
            return (estado, tokenCorrente)
        elif estado != ID:
            if linha[pos] >= '0' and linha[pos] <= '9':
                token += linha[pos]
                pos += 1
            else:
                tokenCorrente = token
                token = ''
                return (estado, tokenCorrente)
        else:
            if linha[pos] >= 'a' and linha[pos] <= 'z':
                token += linha[pos]
                pos += 1
            else:
                tokenCorrente = token
                token = ''
                return (estado,tokenCorrente)

def analisadorLexico():
    global arquivo
    global linhaArquivo
    global pos
    if pos >= len(linhaArquivo) or linhaArquivo[pos] == '\n':
        linhaArquivo = arquivo.readline()
        pos = 0
    if pos < len(linhaArquivo):
        codToken = reconhecedor(linhaArquivo)
        print codToken
        return codToken
    return 0

def analisadorSintatico():
    global pos
    global token
    global pos
    global READ
    global WRITE
    global COMMIT
    global ABORT
    global ID
    global OPEN
    global CLOSE

    codToken = analisadorLexico()
    if not codToken:
        return True
    if codToken[0] == READ or codToken[0] == WRITE:
        codToken = analisadorLexico()
        if codToken[0] == OPEN:
            codToken = analisadorLexico()
            if codToken[0] == ID:
                codToken = analisadorLexico()
                if codToken[0] == CLOSE:
                    analisadorSintatico()
                else:
                    print "Esperava fecha parentes."
                    sys.exit(0)
            else:
                print "Espera dado para operacao."
                sys.exit(0)
        else:
            print "Esperava abre parenteses."
            sys.exit(0)
    elif codToken[0] == COMMIT or codToken[0] == ABORT:
        codToken = analisadorLexico()
        if codToken[0] == OPEN:
            codToken = analisadorLexico()
            if codToken[0] == CLOSE:
                analisadorSintatico()
            else:
                print "Espera fecha parenteses."
                sys.exit(0)
        else:
            print "Esperava abre parenteses."
            sys.exit(0)
    else:
        print "Esperava operacao 'Read', 'Write', 'Commit' ou 'Abort'."
        sys.exit(0)

class DoisPL(object):

    def __init__(self):
        self.operacoes = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes da historia lida
        self.bloqueios = [] #lista de listas com [Transacao, Tipo de Bloqueio, Dado] - informa os bloqueios atuais sobre os dados
        self.desbloqueios = [] #lista de listas com [Transacao, Tipo de Desbloqueio, Dado] - informa os bloqueios que ja foram feitos
        self.historia = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes da historia de saida
        self.delay = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes que estao em delay

    def lerEntrada(self):
        """
        Le a historia de entrada e guarda as operacoes numa lista
        """
        nomeArquivo = 'Historia2pl.txt'
        if not os.path.exists(nomeArquivo):
            print 'Erro no carregamento do arquivo de inicializacao'
            return 0

        arquivo = open(nomeArquivo,'r')
        ind = 0
        for linha in arquivo:
            ind = ind + 1
            for oper in linha.split(' '):
                if len(oper) > 3:
                    operacao = [oper[1], oper[0], oper[3]] #quando eh read e write
                else:
                    operacao = [oper[1], oper[0], ' '] #quando eh commit
                self.operacoes.append(operacao)

        if ind > 1:
            self.montarHistoria(ind)

    def montarHistoria(self, qtde):
        """
        Monta uma historia 'aleatoria'
        """
        listAux = []
        tot = len(self.operacoes)
        while len(listAux) <> tot:
            for i in range(1, qtde + 1):
                for oper in self.operacoes:
                    if int(oper[0]) == i:
                        listAux.append(oper)
                        self.operacoes.remove(oper)
                        break

        for l in listAux:
            self.operacoes.append(l)

    def operacoesEmDelay(self, tran):
        """
        Verifica se uma transacao ja tem operacoes em delay
        """
        for opDelay in self.delay:
            if opDelay[0] == tran:
                return False

        return True

    def tentaObterBloqueio(self, transacao, operacao, dado, modo):
        """
        Tenta bloquear um dado, verificando se este ja esta bloqueado por outra transacao. Se nao conseguir bloquear, a operacao fica em delay
        Se a operacao ja desbloqueou algum dado alguma outra vez, a historia eh invalida (regra do 2pl - transacao nao pode pedir mais de um lock)
        """
        for blocks in self.desbloqueios:
            if blocks[0] == transacao: #transacao ja pediu outro lock anteriormente - invalido
                return 0

        for blocks in self.bloqueios:
            if blocks[2] == dado and blocks[0] <> transacao: #dado bloqueado por outra transacao
                if blocks[1] == 'lx' or operacao == 'w': # soh vai pra delay se tiver block de escrita (leitura e leitura nao)
                    if modo == 0:
                        self.delay.append([transacao, operacao, dado])
                    return 1

        for blocks in self.bloqueios:
            if operacao == 'r':
                if blocks == [transacao, 'ls', dado]: #se a transacao ja tem o lock, ok e nao precisa adicionar nada
                    return 2
            else:
                if blocks == [transacao, 'lx', dado]: #se a transacao ja tem o lock, ok e nao precisa adicionar nada
                    return 2

        #adiciona o seu bloqueio na lista de bloqueios e tambem na historia de execucao
        if operacao == 'r':
            self.bloqueios.append([transacao, 'ls', dado])
            self.historia.append([transacao, 'ls', dado])
        else:
            self.bloqueios.append([transacao, 'lx', dado])
            self.historia.append([transacao, 'lx', dado])

        return 2

    def desbloqueiaDadosTransacao(self, tran):
        """
        Desbloqueia todos os locks que uma transacao possui
        """
        ind = 0
        while ind < len(self.bloqueios):
            block = self.bloqueios[ind]
            if block[0] == tran:
                if block[1] == 'ls':
                    self.historia.append([tran, 'us', block[2]])
                    self.desbloqueios.append([tran, 'us', block[2]])
                    self.bloqueios.remove(block)
                else:
                    self.historia.append([tran, 'ux', block[2]])
                    self.desbloqueios.append([tran, 'ux', block[2]])
                    self.bloqueios.remove(block)
            else:
                ind = ind + 1

    def abortarOperacao(self, tran):
        """
        Aborta uma transacao, limpando seu historico de operacoes e bloqueios e jogando suas operacoes de volta no delay
        """
        listaOperacoes = []
        listaOperacoes[:] = filter(lambda x: x[0]==tran, self.historia) # Transfere operacoes da
        self.historia[:] = filter(lambda x: x[0]!=tran, self.historia) # historia para listaOperacoes,
        listaOperacoes[:] += filter(lambda x: x[0]==tran, self.delay) # o mesmo com o delay e com as operacoes.
        self.delay[:] = filter(lambda x: x[0]!=tran, self.delay) # Sempre nessa ordem para preservar a integridada
        listaOperacoes[:] += filter(lambda x: x[0]==tran, self.operacoes)# da transacao.
        self.operacoes[:] = filter(lambda x: x[0]!=tran, self.operacoes) #
        listaOperacoes = [i for i in listaOperacoes if (i[1]!= 'ux' and i[1]!='us' and i[1]!= 'ls' and i[1]!='lx')]
        self.delay += listaOperacoes
        self.bloqueios[:] = filter(lambda x: x[0]!=tran, self.bloqueios)
        self.desbloqueios[:] = filter(lambda x: x[0]!=tran, self.desbloqueios)

    def executarOperacao(self, operacao, modo):
        """
        Tenta executar a operacao. Modo eh a origem da operacao (0 - operacoes, 1 - delay)
        """
        tran = operacao[0] #transacao
        oper = operacao[1] #operacao
        dado = operacao[2] #dado

        if modo == 0: #operacao normal
            if self.operacoesEmDelay(tran): # verifica se a transacao ja esta em delay
                if oper in ['r', 'w']: #se eh uma operacao de leitura ou escrita
                    ret = self.tentaObterBloqueio(tran, oper, dado, modo) # tenta obter bloqueio sobre o dado
                    if ret == 0:
                        print 'Problemas na operacao %s%s(%s): mais de um pedido de lock pela mesma transacao!' %(oper, tran, dado)
                        print 'Transacao %s abortada!' %(tran)
                        self.abortarOperacao(tran)
                    else:
                        if ret == 2:
                            self.historia.append([tran, oper, dado])
                        self.operacoes.remove([tran, oper, dado])

                else: # se eh commit
                    print 'Transacao %s executada com sucesso!' %(tran)
                    self.desbloqueiaDadosTransacao(tran)
                    self.historia.append([tran, oper, ''])
                    self.operacoes.remove([tran, oper, dado])
            else:
                self.delay.append([tran, oper, dado])
                self.operacoes.remove([tran, oper, dado])
        else:
            if oper in ['r', 'w']: #se eh uma operacao de leitura ou escrita
                ret = self.tentaObterBloqueio(tran, oper, dado, modo) # tenta obter bloqueio sobre o dado
                if ret == 0:
                    print 'Problemas na operacao %s%s(%s): mais de um pedido de lock pela mesma transacao!' %(oper, tran, dado)
                    print 'Transacao %s abortada!' %(tran)
                    self.abortarOperacao(tran)
                else:
                    if ret == 2:
                        self.historia.append([tran, oper, dado])
                        self.delay.remove([tran, oper, dado])
            else: # se eh commit
                print 'Transacao %s executada com sucesso!' %(tran)
                self.desbloqueiaDadosTransacao(tran)
                self.historia.append([tran, oper, ''])
                self.delay.remove([tran, oper, dado])

    def verificaDelay(self, oper, ind):
        """
        Verifica se uma operacao em delay pode ser executada
        """
        tran = oper[0]
        for i in range(0, ind):
            operDelay = self.delay[i]
            if oper <> operDelay:
                if operDelay[0] == tran:
                    return False

        return True

    def verificaCausaAbort(self):
        """
        Verifica se o abort foi por deadlock ou serializabilidade
        """
        list1 = []
        list2 = []
        for operDelay in self.delay:
            for block in self.bloqueios:
                if operDelay[0] <> block[0] and operDelay[2] == block[2]:
                    list1.append([operDelay[0], block[0]])
                    list2.append([operDelay[0], block[0]])
                    break

        for l1 in list1:
            for l2 in list2:
                if l1[0] == l2[1] and l1[1] == l2[0]:
                    return False

        return True

    def pegaOperacoesDelay(self, modo):
        """
        Pega as operacoes que estao em delay
        """
        ind = 0
        totDelay = len(self.delay)
        totDelInd = 0
        for i in range(0, totDelay):
            totDelInd = len(self.delay)
            operacaoDelay = self.delay[ind]
            if self.verificaDelay(operacaoDelay, ind):
                self.executarOperacao(operacaoDelay, 1)
            if totDelInd == len(self.delay):
                ind = ind + 1

        if modo == 1:
            # se for igual eh porque nenhuma transacao em delay conseguiu ser executada, entao tem que abortar
            if totDelay == len(self.delay):
                if self.verificaCausaAbort():
                    print 'Transacao %s abortada por conflito de serializabilidade!' %(self.delay[0][0])
                else:
                    print 'Transacao %s abortada por deadlock!' %(self.delay[0][0])
                self.abortarOperacao(self.delay[0][0])

    def pegaOperacoes(self):
        """
        Parte principal: a partir das operacoes da historia de entrada, tenta montar a historia de saida
        """
        while len(self.operacoes) > 0:
            operacao = self.operacoes[0]
            self.executarOperacao(operacao, 0)

            #a cada operacao tenta executar tambem as que estao em delay (se houver)
            self.pegaOperacoesDelay(0)

        while len(self.delay) > 0:
            self.pegaOperacoesDelay(1)

    def escreveHistoria(self):
        """
        Le a lista da historia de saida e escreve na tela
        """
        print '\nHistoria de Execucao:'
        for elemento in self.historia:
            if len(elemento[2]) > 0:
                print '%s%s(%s)' %(elemento[1], elemento[0], elemento[2])
            else:
                print '%s%s' %(elemento[1], elemento[0])


def main():
    nomeArquivo = 'Historia2pl.txt'
    if not os.path.exists(nomeArquivo):
        print 'Erro no carregamento do arquivo de inicializacao'
        return 0

    global arquivo
    arquivo = open(nomeArquivo,'r')
    analise = analisadorSintatico()
    print ''
    doispl = DoisPL()
    doispl.lerEntrada()
    doispl.pegaOperacoes()
    doispl.escreveHistoria()

if __name__ == "__main__":
    main()