# Generated from sparsecsv.g4 by ANTLR 4.7.1
# encoding: utf-8
from __future__ import print_function
from antlr4 import *
from io import StringIO
import sys


def serializedATN():
    with StringIO() as buf:
        buf.write(u"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2")
        buf.write(u"\13.\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7")
        buf.write(u"\t\7\4\b\t\b\4\t\t\t\4\n\t\n\3\2\3\2\3\3\3\3\3\4\3\4")
        buf.write(u"\3\5\3\5\3\6\3\6\3\7\6\7!\n\7\r\7\16\7\"\3\b\6\b&\n\b")
        buf.write(u"\r\b\16\b\'\3\t\3\t\3\n\3\n\3\n\2\2\13\3\3\5\4\7\5\t")
        buf.write(u"\6\13\7\r\b\17\t\21\n\23\13\3\2\3\b\2\f\f$$..^^}}\177")
        buf.write(u"\177\2/\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2")
        buf.write(u"\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2")
        buf.write(u"\2\23\3\2\2\2\3\25\3\2\2\2\5\27\3\2\2\2\7\31\3\2\2\2")
        buf.write(u"\t\33\3\2\2\2\13\35\3\2\2\2\r \3\2\2\2\17%\3\2\2\2\21")
        buf.write(u")\3\2\2\2\23+\3\2\2\2\25\26\7}\2\2\26\4\3\2\2\2\27\30")
        buf.write(u"\7\177\2\2\30\6\3\2\2\2\31\32\7*\2\2\32\b\3\2\2\2\33")
        buf.write(u"\34\7+\2\2\34\n\3\2\2\2\35\36\7.\2\2\36\f\3\2\2\2\37")
        buf.write(u"!\7$\2\2 \37\3\2\2\2!\"\3\2\2\2\" \3\2\2\2\"#\3\2\2\2")
        buf.write(u"#\16\3\2\2\2$&\n\2\2\2%$\3\2\2\2&\'\3\2\2\2\'%\3\2\2")
        buf.write(u"\2\'(\3\2\2\2(\20\3\2\2\2)*\7\f\2\2*\22\3\2\2\2+,\7^")
        buf.write(u"\2\2,-\7.\2\2-\24\3\2\2\2\5\2\"\'\2")
        return buf.getvalue()


class sparsecsvLexer(Lexer):

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    LBRACE = 1
    RBRACE = 2
    LPAREN = 3
    RPAREN = 4
    COMMA = 5
    QUOTES = 6
    STRING = 7
    NEWLINE = 8
    BSCOMMA = 9

    channelNames = [ u"DEFAULT_TOKEN_CHANNEL", u"HIDDEN" ]

    modeNames = [ u"DEFAULT_MODE" ]

    literalNames = [ u"<INVALID>",
            u"'{'", u"'}'", u"'('", u"')'", u"','", u"'\n'", u"'\\,'" ]

    symbolicNames = [ u"<INVALID>",
            u"LBRACE", u"RBRACE", u"LPAREN", u"RPAREN", u"COMMA", u"QUOTES", 
            u"STRING", u"NEWLINE", u"BSCOMMA" ]

    ruleNames = [ u"LBRACE", u"RBRACE", u"LPAREN", u"RPAREN", u"COMMA", 
                  u"QUOTES", u"STRING", u"NEWLINE", u"BSCOMMA" ]

    grammarFileName = u"sparsecsv.g4"

    def __init__(self, input=None, output=sys.stdout):
        super(sparsecsvLexer, self).__init__(input, output=output)
        self.checkVersion("4.7.1")
        self._interp = LexerATNSimulator(self, self.atn, self.decisionsToDFA, PredictionContextCache())
        self._actions = None
        self._predicates = None


