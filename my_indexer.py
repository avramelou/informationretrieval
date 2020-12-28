# -*- coding: utf-8 -*-
import numpy as np


class InvertedIndexer:

    #  The name of the file that has the documents data
    DOC_DATA_FILENAME = "data.npy"

    def __init__(self, new_indexer=True):
        # The numbers of the total different documents
        self.total_documents = None

        # Dictionary for indexer
        self.indexer = {}

        if new_indexer:
            self.create_new_inverted_indexer_file()
        else:
            self.indexer = np.load("data.npy", allow_pickle=True).item()

    def create_new_inverted_indexer_file(self):
        # load data file from crawler
        document_dict = np.load(InvertedIndexer.DOC_DATA_FILENAME, allow_pickle=True).item()

        self.total_documents = len(document_dict.items())

        # create indexer
        for doc, words in document_dict.items():
            # find all unique words in doc
            unique_words = set(words.split())
            # find positions of every unique word
            words_pos = []
            for x in unique_words:
                words_pos.append([i for i, w in enumerate(words.split()) if x == w])
            # zip words with their positions in doc
            doc_words = zip(unique_words, words_pos)
            # update indexer with words in doc
            for word, pos in doc_words:
                # word doesn't exist add it in indexer { word : (1,(doc,position)) }
                if word not in self.indexer:
                    self.indexer[word] = (1, (doc, pos))
                # if word exists update record like this {word : ((n+1),...docs...,(doc,position)}
                else:
                    # get record for the word
                    values = self.indexer.get(word)
                    values_l = list(values)  # transform tuple to list in order to change it
                    n = values[0]
                    values_l[0] = n + 1
                    values_l.append((doc, pos))
                    values = tuple(values_l)
                    self.indexer.update({word: values})  # update indexer

        print(self.indexer)
        np.save("inverter_indexer.npy", self.indexer)