#-------------------------------------------------------------------------------
# Nome:        2PL
# Autores:     Luiz Eduardo
#              Matheus Pereira
#
# Criado:      13/10/2012
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
        self.operacoes    = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes da historia lida
        self.bloqueios    = [] #lista de listas com [Transacao, Tipo de Bloqueio, Dado] - informa os bloqueios atuais sobre os dados
        self.desbloqueios = [] #lista de listas com [Transacao, Tipo de Desbloqueio, Dado] - informa os bloqueios que ja foram feitos
        self.historia     = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes da historia de saida
        self.delay        = [] #lista de listas com [Transacao, Operacao, Dado] - operacoes que estao em delay

    def transfereEntrada(self, historia):
        """
            Transfere o conteudo da historia pra uma lista de operacoes
        """
        for oper in historia.split(' '):
            if len(oper) > 3:
                operacao = [oper[1], oper[0], oper[3]] #quando eh read e write
            else:
                operacao = [oper[1], oper[0], ''] #quando eh commit
            self.operacoes.append(operacao)

    def lerEntrada(self):
        """
            Le a historia de entrada e guarda as operacoes numa lista
        """
        modLeitura = input('Digite o modo de entrada de dados:\n1 - Arquivo\n2 - Digitado\n')

        if modLeitura == 1:
#            nomeArquivo = raw_input('Digite o nome do arquivo: ')
            nomeArquivo = 'Historia2pl.txt'
            if not os.path.exists(nomeArquivo):
                print 'Erro no carregamento do arquivo de inicializacao'
                return 0

            arquivo = open(nomeArquivo,'r')
            for linha in arquivo:
                self.transfereEntrada(linha)
        else:
            entrada = input('Digite a historia de execucao:\n')
            self.transfereEntrada(entrada)

    def operacoesEmDelay(self, tran):
        """
            Verifica se uma transacao ja tem operacoes em delay
        """
        for opDelay in self.delay:
            if opDelay[0] == tran:
                return False

        return True

    def tentaObterBloqueio(self, transacao, operacao, dado):
        """
            Tenta bloquear um dado, verificando se este ja esta bloqueado por outra transacao. Se nao conseguir bloquear, a operacao fica em delay
            Se a operacao ja desbloqueou algum dado alguma outra vez, a historia eh invalida (regra do 2pl - transacao nao pode pedir mais de um lock)
        """
        for blocks in self.desbloqueios:
            if blocks[0] == transacao: #transacao ja pediu outro lock anteriormente - invalido
                return False

        canBlo = True
        for blocks in self.bloqueios:
            if blocks[2] == dado and blocks[0] <> transacao: #dado bloqueado por outra transacao - operacao vai pra delay
                self.delay.append([transacao, operacao, dado])
                canBlo = False
                break

        if canBlo: #adiciona o seu bloqueio na lista de bloqueios e tambem na historia de execucao
            if operacao == 'r':
                self.bloqueios.append([transacao, 'ls', dado])
                self.historia.append([transacao, 'ls', dado])
            else:
                self.bloqueios.append([transacao, 'lx', dado])
                self.historia.append([transacao, 'lx', dado])

        return True

    def desbloqueiaDadosTransacao(self, tran):
        """
            Desbloqueia todos os locks que uma transacao possui
        """
        for block in self.bloqueios:
            if block[0] == tran:
                if block[1] == 'ls':
                    self.historia.append([tran, 'us', block[2]])
                    self.desbloqueios.append([tran, 'us', block[2]])
                    self.bloqueios.remove(block)
                else:
                    self.historia.append([tran, 'ux', block[2]])
                    self.desbloqueios.append([tran, 'ux', block[2]])
                    self.bloqueios.remove(block)
        
    def executarOperacao(self, operacao):
        """
            Tenta executar a operacao
        """
        tran = operacao[0] #transacao
        oper = operacao[1] #operacao
        dado = operacao[2] #dado

        if self.operacoesEmDelay(tran): # verifica se a transacao ja esta em delay
            if oper in ['r', 'w']: #se eh uma operacao de leitura ou escrita            
                if not self.tentaObterBloqueio(tran, oper, dado): # tenta obter bloqueio sobre o dado
                    print 'Problemas na operacao %s%s(%s): mais de um pedido de lock pela mesma transacao!' %(tran, oper, dado)
                    return False
                else:
                    self.historia.append([tran, oper, dado])
                    self.operacoes.remove([tran, oper, dado])
                
            else: # se eh commit
                self.desbloqueiaDadosTransacao(tran)
                self.historia.append([tran, oper, dado])
                self.operacoes.remove([tran, oper, dado])
        else:
            self.delay.append([tran, oper, dado])
            self.operacoes.remove([tran, oper, dado])
        
        return True

    def pegaOperacoes(self):
        """
            Parte principal: a partir das operacoes da historia de entrada, tenta montar a historia de saida
        """
        for operacao in self.operacoes:
            if not self.executarOperacao(operacao):
                return False
            #a cada operacao tenta executar tambem as que estao em delay (se houver)
            for operacaoDelay in self.delay:
                if not self.executarOperacao(operacaoDelay):
                    return False
        
        while len(self.delay) > 0:
            for operacaoDelay in self.delay:
                if not self.executarOperacao(operacaoDelay):
                    return False
        
        return True

    def escreveHistoria(self):
        """
            Le a lista da historia de saida e escreve na tela
        """
        for elemento in self.historia:
            if len(elemento[2]) > 0:
                print '%s%s(%s)' %(elemento[1], elemento[0], elemento[2])
            else:
                print '%s%s' %(elemento[1], elemento[0])


#doispl = DoisPL()
#doispl.lerEntrada()
#doispl.pegaOperacoes()
#doispl.escreveHistoria()

def main():
#   nomeArquivo = raw_input('Digite o nome do arquivo: ')
    nomeArquivo = 'Historia2pl.txt'
    if not os.path.exists(nomeArquivo):
        print 'Erro no carregamento do arquivo de inicializacao'
        return 0

    global arquivo
    arquivo = open(nomeArquivo,'r')
    analise = analisadorSintatico()
    if analise:
        print 'Ok.'
    return 0

if __name__ == "__main__":
    main()
