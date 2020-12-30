# -*- coding: utf-8 -*-
import numpy as np
import collections
import math
import threading
import time

class InvertedIndexer(threading.Thread):

    #  The name of the file that has the documents data
    DOC_DATA_FILENAME = "data.npy"
    
    #global variables
    indexer = {}  
    documents_data = {}
    document_dict = {}
    count = None #num of documents that we have visited
    
    #locks for global variables
    lock_indexer = threading.Lock()
    lock_documents_data = threading.Lock()
    lock_count = threading.Lock()
    
    @staticmethod
    def init_static_variables():
        #load data from file
        InvertedIndexer.document_dict = np.load(InvertedIndexer.DOC_DATA_FILENAME,allow_pickle=True).item()
        InvertedIndexer.count = 0

    def __init__(self, new_indexer=True):
        #check if we start over or we start from existed indexer
        if(new_indexer==False):
            InvertedIndexer.indexer = np.load("inverted_indexer.npy",allow_pickle=True).item()
            InvertedIndexer.documents_data = np.load("documents_data.npy",allow_pickle=True).item()
        threading.Thread.__init__(self) 

    #function that create indexer
    def run(self):
        # while loop because we want to run this function multiple times 
        while(True):
            InvertedIndexer.lock_count.acquire()
            if(InvertedIndexer.count<len(InvertedIndexer.document_dict)):
                #check if we have already visited this document
                while(True):
                    doc,words = list(InvertedIndexer.document_dict.items())[InvertedIndexer.count]
                    InvertedIndexer.count+=1
                    if(InvertedIndexer.count>=len(InvertedIndexer.document_dict)):
                        InvertedIndexer.lock_count.release()
                        return
                    if(doc not in InvertedIndexer.documents_data.keys()):
                        InvertedIndexer.lock_count.release()
                        break
                for pos,word in enumerate(words.split()):
                    # word doesn't exist add it in indexer { word : (1,(doc,position)) }
                    InvertedIndexer.lock_indexer.acquire()
                    if word not in InvertedIndexer.indexer:
                        InvertedIndexer.indexer[word] = (1, (doc, pos))
                        InvertedIndexer.lock_indexer.release()
                        # if word exists update record like this {word : ((n+1),...docs...,(doc,position)}
                    else:
                        # get record for the word
                        values = InvertedIndexer.indexer.get(word)
                        InvertedIndexer.lock_indexer.release()
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
                        InvertedIndexer.lock_indexer.acquire()
                        InvertedIndexer.indexer.update({word: values}) #update indexer
                        InvertedIndexer.lock_indexer.release()
                #find the maxfreq and Ld of document
                freq = collections.Counter(words)
                maxfreq = max(freq.values()) if len(freq.values())!=0  else 0
                ld = math.sqrt(sum([x**2 for x in freq.values()]))*maxfreq
                InvertedIndexer.lock_documents_data.acquire()
                InvertedIndexer.documents_data[doc] = (ld,maxfreq)
                InvertedIndexer.lock_documents_data.release()
            else:
                InvertedIndexer.lock_count.release()
                break
        
        
        
        
indexer_threads = []
InvertedIndexer.init_static_variables()
index_start_time = time.time()
for i in range(2):
    indexer = InvertedIndexer()
    indexer.start()
    indexer_threads.append(indexer)
for indexer in indexer_threads:
    indexer.join()
index_end_time = time.time()
print(InvertedIndexer.indexer)
print("Time needed: %f seconds" %(index_end_time-index_start_time))
np.save("inverted_indexer.npy", InvertedIndexer.indexer)
np.save("documents_data.npy",InvertedIndexer.documents_data)