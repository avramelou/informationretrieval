# -*- coding: utf-8 -*-
import numpy as np
import collections
import math
import threading
import time

class InvertedIndexer(threading.Thread):

    #  The name of the file that has the documents data
    DOC_DATA_FILENAME = "data.npy"
    
    #gloabal variables
    indexer = {} 
    documents_data = {}
    document_dict = {}
    count = None
    
    #lock for global variables
    lock = threading.Lock()
    
    @staticmethod
    def init_static_variables():
        #load data from file
        InvertedIndexer.document_dict = np.load(InvertedIndexer.DOC_DATA_FILENAME,allow_pickle=True).item()
        InvertedIndexer.count = 0

    def __init__(self, new_indexer=True):  
        threading.Thread.__init__(self)

    def run(self):
        # create indexer
        InvertedIndexer.lock.acquire()
        while (InvertedIndexer.count<len(InvertedIndexer.document_dict)):
            doc,words = list(InvertedIndexer.document_dict.items())[InvertedIndexer.count]
            for pos,word in enumerate(words.split()):
                # word doesn't exist add it in indexer { word : (1,(doc,position)) }
                if word not in InvertedIndexer.indexer:
                    InvertedIndexer.indexer[word] = (1, (doc, pos))
                # if word exists update record like this {word : ((n+1),...docs...,(doc,position)}
                else:
                    # get record for the word
                    values = InvertedIndexer.indexer.get(word)
                    valuesL = list(values) #transform tuple to list in order to change it
                    #if we have already see the word in doc
                    if(doc in valuesL[1:][0]):
                        temp = valuesL[1:][0].index(doc)
                        positionsL = list(valuesL[temp+1]) # we use temp+1 because we search from 1....n-1 elements of the list
                        positionsL.append(pos)
                        positions = tuple(positionsL)
                        valuesL[temp+1] = positions
                    #if this is the first appereance of word in doc 
                    else:
                        n = values[0]
                        valuesL[0] = n+1
                        valuesL.append((doc,pos))
                    values = tuple(valuesL)
                    InvertedIndexer.indexer.update({word: values}) #update indexer
            #find the maxfreq of document
            freq = collections.Counter(words)
            maxfreq = max(freq.values()) if len(freq.values())!=0  else 0
            ld = math.sqrt(sum([x**2 for x in freq.values()]))*maxfreq
            InvertedIndexer.documents_data[doc] = (ld,maxfreq)
            InvertedIndexer.count+=1
        InvertedIndexer.lock.release()
        
        
        
        
indexer_threads = []
InvertedIndexer.init_static_variables()
index_start_time = time.time()
for i in range(4):
    indexer = InvertedIndexer()
    indexer.start()
    indexer_threads.append(indexer)
for indexer in indexer_threads:
    indexer.join()
index_end_time = time.time()
print(InvertedIndexer.indexer)
np.save("inverted_indexer.npy", InvertedIndexer.indexer)
np.save("documents_data.npy",InvertedIndexer.documents_data)