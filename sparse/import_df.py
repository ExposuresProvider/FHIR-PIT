import numpy as np
import pandas as pd
from antlr4 import *
from sparsecsvLexer import sparsecsvLexer
from sparsecsvParser import sparsecsvParser
from sparsecsvListener import sparsecsvListener
from antlr4.tree.Trees import Trees
from enum import Enum

def load_df(filepath):
    colnames_dict = {
        u"icd" : import_array("icd_meta.csv"),
        u"mdctn" : import_array("mdctn_meta.csv"),
        u"loinc" : import_array("loinc_meta.csv")
        }
    def meta(x):
        for key, val in colnames_dict.iteritems():
            if x[:len(key)] == key:
                return map(lambda y: x + "_" + y, val)
        return x
    colnames = map(meta, import_array("endotype_meta.csv"))

    return import_sparse(colnames, filepath)

def wrap(x):
    if not isinstance(x, list):
        return [x]
    else:
        return x
    
class sparsecsvListener(sparsecsvListener):
    def __init__(self, colnames):
        self.colnames = colnames
        self.stack = []
        self.df = pd.DataFrame(columns=sum(map(wrap, colnames),[])) #.to_sparse()
    def enterRow(self, ctx):
        self.colgroup = 0
        self.row = {}
        self.col = 0
    def exitRow(self, ctx):
        df = pd.DataFrame([self.row]) #.to_sparse()
        self.df = self.df.append(df)
    def enterEntry(self, ctx):
        self.isArray = False
        self.stack = []
    def exitEntry(self, ctx):
        if not self.isArray:
            colname = self.colnames[self.col]
            self.row[colname] = self.stack[0]
    def enterCol(self, ctx):
        self.col += 1
    def enterElements(self, ctx):
        self.stack = []
    def exitElements(self, ctx):
        self.elements = self.stack
    def enterIndices(self, ctx):
        self.stack = []
    def exitIndices(self, ctx):
        self.indices = self.stack
    def enterString(self, ctx):
        self.stack.append(ctx.getText())
    def enterSparse_array(self,ctx):
        self.isArray = True
    def exitSparse_array(self,ctx):
        indices = map(int, self.indices)
        elements = self.elements
        names = self.colnames[self.col]
        for inx, name in enumerate(names):
            if inx in indices:
                self.row[name] = elements[indices.index(inx)]
            else:
                self.row[name] = u""

class arrayListener(sparsecsvListener):
    def __init__(self):
        self.elements = []
    def enterString(self, ctx):
        self.elements.append(ctx.getText())

def import_sparse(colnames, filepath):
    input = FileStream(filepath)
    lexer = sparsecsvLexer(input)
    stream = CommonTokenStream(lexer)
    parser = sparsecsvParser(stream)
    tree = parser.csv()
    walker = ParseTreeWalker()
    listener = sparsecsvListener(colnames)
    walker.walk(listener, tree)
    return listener.df
         
def import_array(filepath):
    input = FileStream(filepath)
    lexer = sparsecsvLexer(input)
    stream = CommonTokenStream(lexer)
    parser = sparsecsvParser(stream)
    tree = parser.array()
    walker = ParseTreeWalker()
    listener = arrayListener()
    walker.walk(listener, tree)
    return listener.elements

