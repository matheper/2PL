#-------------------------------------------------------------------------------
# Nome:        2PL
# Autores:     Luiz Eduardo
#              Matheus Pereira
#
# Criado:      13/10/2012
#-------------------------------------------------------------------------------

import sys
import os.path

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
        
    def executarOperacao(self, operacao):
        """
            Tenta executar a operacao
        """
        tran = operacao[0] #transacao
        oper = operacao[1] #operacao
        dado = operacao[2] #dado
        if oper in ['r', 'w']:
            if not self.tentaObterBloqueio(tran, oper, dado):
                print 'Problemas na operacao %s%s(%s): mais de um pedido de lock pela mesma transacao!' %(tran, oper, dado)
                return False
            else:
                self.historia.append([tran, oper, dado])
        else:
            self.historia.append([tran, oper, dado])
        
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


doispl = DoisPL()
doispl.lerEntrada()
doispl.pegaOperacoes()
doispl.escreveHistoria()

