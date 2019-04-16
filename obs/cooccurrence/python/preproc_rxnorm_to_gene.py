import pandas as pd;

medname_rxnorm = pd.read_csv("map.medname-rxnorm.tsv", sep="\t")

medname_gene = pd.read_csv("drug20-gene.tsv", sep="\t", header=None, names = ["MED_NAME", "GENE_NAME"])

rxnorm_gene = medname_rxnorm.merge(medname_gene, on = "MED_NAME")[["RXNORM_CUI", "GENE_NAME"]]

rxnorm_gene.to_csv("rxnorm_gene_map", sep=" ", header=None, index=False)
