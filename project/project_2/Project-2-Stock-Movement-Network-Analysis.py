# -*- coding: utf-8 -*-
"""
Created on Sat Oct 10 16:53:31 2020

@author: arnou
"""


import networkx as nx
from nxviz import CircosPlot
import pandas as pd
import numpy as np
import collections
import matplotlib.pyplot as plt
import matplotlib
#%matplotlib inline
from itertools import count
import warnings
from pyvis import network as net

warnings.filterwarnings('ignore')

stocks = pd.read_csv('https://raw.githubusercontent.com/myvioletrose/data620_team/master/project/project_2/sp500_Sep2020.csv',
                 sep=',',
                 header=1,
                 names=['symbol','datekey','close','previous_close','direction','company_name','sector'],
                 encoding="ISO-8859-1")

stocks2 = stocks

new_df = pd.merge(stocks, stocks2,  how='outer', left_on=['datekey','direction'], right_on = ['datekey','direction'])

new_df = new_df[(new_df.symbol_x != new_df.symbol_y)]

new_df = new_df[(new_df.direction != "Same")]

new_df["name_one_length"] = new_df['company_name_x'].str.len()
new_df["name_two_length"] = new_df['company_name_y'].str.len()

new_df['symbol_one'] = new_df['symbol_x']
new_df['symbol_two'] = new_df['symbol_y']


new_df['company_name_one'] = new_df['company_name_x']
new_df['company_name_two'] = new_df['company_name_y']

new_df['sector_one'] = new_df['sector_x']
new_df['sector_two'] = new_df['sector_y']

#####################################################################################################
new_df.loc[new_df.name_one_length > new_df.name_two_length,'symbol_one'] = new_df['symbol_x']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'symbol_one'] = new_df['symbol_y']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'symbol_two'] = new_df['symbol_x']
new_df.loc[new_df.name_one_length > new_df.name_two_length,'symbol_two'] = new_df['symbol_y']
#####################################################################################################
new_df.loc[new_df.name_one_length > new_df.name_two_length,'company_name_one'] = new_df['company_name_x']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'company_name_one'] = new_df['company_name_y']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'company_name_two'] = new_df['company_name_x']
new_df.loc[new_df.name_one_length > new_df.name_two_length,'company_name_two'] = new_df['company_name_y']
#####################################################################################################
#####################################################################################################
new_df.loc[new_df.name_one_length > new_df.name_two_length,'sector_one'] = new_df['sector_x']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'sector_one'] = new_df['sector_y']
new_df.loc[new_df.name_one_length < new_df.name_two_length,'sector_two'] = new_df['sector_x']
new_df.loc[new_df.name_one_length > new_df.name_two_length,'sector_two'] = new_df['sector_y']
#####################################################################################################

new_df['sector_x'] = new_df['sector_one']
new_df['sector_y'] = new_df['sector_two']

new_df['symbol_x'] = new_df['symbol_one']
new_df['symbol_y'] = new_df['symbol_two']

new_df['company_name_x'] = new_df['company_name_one']
new_df['company_name_y'] = new_df['company_name_two']

new_df = new_df.loc[:,['datekey','symbol_x', 'symbol_y','company_name_x','company_name_y','sector_x','sector_y']]

new_df = new_df.drop_duplicates()

weights = new_df.loc[:,['symbol_x', 'symbol_y','sector_x']].groupby(['symbol_x', 'symbol_y']).count().reset_index()

weights.columns = ['symbol_x', 'symbol_y', 'freq']

new_df = new_df.loc[:,['symbol_x', 'symbol_y','company_name_x','company_name_y','sector_x','sector_y']]

new_df = new_df.drop_duplicates()


new_df= pd.merge(new_df, weights,  how='outer', left_on=['symbol_x','symbol_y'], right_on = ['symbol_x','symbol_y'])


new_df2 = new_df

maxdf = new_df2.groupby(['company_name_x'])['freq'].max()


new_df = new_df[(new_df.freq>1)]



########################################## TRYING TO FIGURE OUT WHICH STOCKS HAVE NO CONNETIONS TO THE REST OF THE MARKET ###############################################################
no_matches = pd.merge(new_df2, weights,  how='outer', left_on=['symbol_x'], right_on = ['symbol_x'])

no_matches = no_matches.freq_x.null()


weights2 = weights.symbol_x
weights3 = weights.symbol_y
weights4 = weights2.append(weights3)

weights4.nunique()

stocks.symbol.nunique()


weights['symbol'] = weights['symbol_x']

weights2 = weights.loc[:,['symbol']].reset_index()


stocks2 = stocks.loc[:,['symbol']].reset_index()


stocks.where(stocks.symbol==weights.symbol)

#not_in = stocks[~stocks.isin(weights.to_dict('l')).all(1)]

#not_in = stocks2.drop(stocks2.merge(weights2).index)

not_in = pd.merge(stocks,weights, on = 'symbol',how = 'left')

not_in2 = not_in.loc[pd.isnull(not_in.freq)]

not_in = not_in.loc[:,['symbol']]

not_in = not_in.symbol.drop_duplicates

len(not_in2.symbol.unique())

not_in2.symbol.unique()

#######################################################################################################################################################################
#######################################################################################################################################################################
#######################################################################################################################################################################


class_net = net.Network(height="100%", width="100%", bgcolor="#222222",font_color="white",notebook=False)
# set the physics layout of the network
class_net.barnes_hut()
#got_data = pd.read_csv("https://www.macalester.edu/~abeverid/data/stormofswords.csv")




new_df['symbol_x'] = new_df.symbol_x.astype(str)
new_df['symbol_y'] = new_df.symbol_y.astype(str)
new_df['sector_x'] = new_df.sector_x.astype(str)
new_df['sector_y'] = new_df.sector_y.astype(str)
#new_df['dept'] = new_df.dept.astype(str)
#new_df['dept_2'] = new_df.dept_2.astype(str)


sources = new_df.iloc[:,2]
targets = new_df.iloc[:,3]
weights = new_df.iloc[:,6]
source_genders = new_df.iloc[:,4]
target_genders = new_df.iloc[:,5]
#source_class = new_df.iloc[:,4]
#target_class = new_df.iloc[:,6]
edge_data = zip(sources, targets, weights,source_genders,target_genders)

for e in edge_data:
    src = e[0]
    dst = e[1]
    w = e[2]
    srcgen = e[3]
    targen = e[4]
    #srcclass = e[5]
    #tarclass = e[6]
    
    class_net.add_node(src, src, title=src, group = (srcgen))
    class_net.add_node(dst, dst, title=dst, group = (targen))
    class_net.add_edge(src, dst, value=w)


neighbor_map = class_net.get_adj_list()
# add neighbor data to node hover data
for node in class_net.nodes:
    node["title"] = node["group"]+ " " + node["title"] + "'s Neighbors:<br>" + "<br>".join(neighbor_map[node["id"]])
    node["value"] = len(neighbor_map[node["id"]])
class_net.show("stocks2.html")











