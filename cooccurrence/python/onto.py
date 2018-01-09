import requests
import gzip
import pandas as pd
import networkx as nx
import numpy as np
import igraph
import sys

# import seaborn as sns
import ddot
from ddot import Ontology, nx_to_NdexGraph, read_term_descriptions, ndex_to_sim_matrix, expand_seed, melt_square, align_hierarchies, parse_ndex_uuid

alpha = float(sys.argv[1])
beta = float(sys.argv[2])

singleton = sys.argv[3]
cooc = sys.argv[4]
output = sys.argv[5]

singleton_df = pd.read_csv(singleton,sep=" ",header=None)
cooc_df = pd.read_csv(cooc,sep=" ", header = None)
frequency_dict = singleton_df.set_index(0).to_dict()[1]

print(frequency_dict)

# print(singleton_df)
# print(cooc_df)

def normalize(x):
    concept1 = x[0]
    concept2 = x[1]
    x[0] = x[0].replace(':', '_')
    x[1] = x[1].replace(':', '_')
    x[2] = float(x[2])/(frequency_dict[concept1] * frequency_dict[concept2])
#    print(x)
    return x
    
cooc_df = cooc_df.apply(normalize, axis=1)

print(cooc_df)

print(isinstance(cooc_df, pd.DataFrame))

ont = Ontology.run_clixo(cooc_df, alpha, beta)

print(ont.to_table(output, clixo_format=True))
# nwx = ont.to_networkx()
# nx.write_graphml(nwx, 'clixotable.graphml')

grph = ont.to_igraph(include_genes=True)
grph.write_graphml(output+'.graphml')
