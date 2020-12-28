# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 22:03:19 2020

@author: User
"""

import numpy as np


#load data file from crawler
page_dict = np.load("data.npy", allow_pickle=True).item()

#indexer
indexer = {}


#create indexer 
for doc,words in page_dict.items():
    #find all unique words in doc 
    unique_words = set(words.split())
    # find positions of every unique word
    words_pos = []
    for x in unique_words:
        words_pos.append([i for i,w in enumerate(words.split()) if x == w])
    # zip words with their positions in doc
    doc_words = zip(unique_words,words_pos)
    # update indexer with words in doc
    for word,pos in doc_words:
        # word doesn't exist add it in indexer { word : (1,(doc,position)) }
        if(word not in indexer):
            indexer[word] = (1,(doc,pos))
        # if word exists update record like this {word : ((n+1),...docs...,(doc,position)}
        else:
            # get record for the word
            values = indexer.get(word)
            valuesL = list(values) #transform tuple to list in order to change it
            n = values[0]
            valuesL[0] = n+1
            valuesL.append((doc,pos))
            values = tuple(valuesL)
            indexer.update({word: values}) #update indexer