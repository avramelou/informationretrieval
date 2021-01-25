# -*- coding: utf-8 -*-
import numpy as np
import collections
import math
import threading
import time

import queue


class InvertedIndexer(threading.Thread):

    # The name of the file that has the documents data
    DOC_DATA_FILENAME = "data.npy"
    # The name of the file that has inverted indexer
    INVERTED_INDEXER_FILENAME = "inverted_indexer.npy"
    # The name of the file that has the documents metadata
    DOC_METADATA_FILENAME = "documents_metadata.npy"

    # Static-class variables
    indexer = {}
    documents_metadata = {}
    document_dict = {}
    counted_documents = None  # Number of documents that have been visited

    document_queue = queue.Queue()

    # Locks for global(static) variables
    lock_indexer = threading.Lock()
    lock_documents_data = threading.Lock()
    lock_count = threading.Lock()

    # Initialising indexer and it's static variables for the indexer
    @staticmethod
    def init_indexer(new_indexer=True):
        # Load data from file
        InvertedIndexer.document_dict = np.load(InvertedIndexer.DOC_DATA_FILENAME, allow_pickle=True).item()
        InvertedIndexer.counted_documents = 0

        # Check if we start over or we start from existed indexer
        if not new_indexer:
            InvertedIndexer.indexer = np.load(InvertedIndexer.INVERTED_INDEXER_FILENAME, allow_pickle=True).item()
            InvertedIndexer.documents_metadata = np.load(InvertedIndexer.DOC_METADATA_FILENAME, allow_pickle=True).item()

        for k, v in InvertedIndexer.document_dict.items():
            pair = (k, v)
            InvertedIndexer.document_queue.put(pair)

    def __init__(self):
        threading.Thread.__init__(self)
        self.docs_processed = 0

    # Method that creates the indexer
    def run(self):
        # While loop because we want to run this function multiple times
        while True:
            InvertedIndexer.lock_count.acquire()
            if InvertedIndexer.document_queue.empty() is False:

                doc, (title, words) = InvertedIndexer.document_queue.get()
                InvertedIndexer.lock_count.release()

                # Checks to find only unvisited documents
                # (in the case of running the algorithm for another time after the inverted dictionary was already made)
                InvertedIndexer.lock_documents_data.acquire()
                if doc in InvertedIndexer.documents_metadata.keys():
                    InvertedIndexer.lock_documents_data.release()
                    continue
                InvertedIndexer.lock_documents_data.release()

                self.docs_processed += 1
                InvertedIndexer.lock_indexer.acquire()
                for pos, word in enumerate(words):
                    # word doesn't exist add it in indexer { word : (1,(doc,position)) }
                    # InvertedIndexer.lock_indexer.acquire()
                    if word not in InvertedIndexer.indexer:
                        InvertedIndexer.indexer[word] = (1, (doc, pos))
                        # InvertedIndexer.lock_indexer.release()
                        # If the word exists, then update record like this {word : ((n+1),...docs...,(doc,position)}
                    else:
                        # Get record for the word
                        values = InvertedIndexer.indexer.get(word)
                        # InvertedIndexer.lock_indexer.release()
                        values_l = list(values)  # Transform tuple to list in order to change it
                        # If the word was already found in the document doc
                        doc_l = [x[0] for x in values_l[1:]]
                        if doc in doc_l:
                            temp = doc_l.index(doc)
                            # We use temp+1 because we search from 1....n-1 elements of the list
                            positions_l = list(values_l[temp+1])
                            positions_l.append(pos)
                            positions = tuple(positions_l)
                            values_l[temp+1] = positions
                            # If this is the first appearance of word in doc
                        else:
                            n = values[0]
                            values_l[0] = n+1
                            values_l.append((doc, pos))
                        values = tuple(values_l)
                        # InvertedIndexer.lock_indexer.acquire()
                        InvertedIndexer.indexer.update({word: values})  # Update indexer
                        # InvertedIndexer.lock_indexer.release()
                InvertedIndexer.lock_indexer.release()
                # Find the max_freq and Ld of document
                freq = collections.Counter(words)

                if len(freq.values()) != 0:
                    max_freq = max(freq.values())
                    #  The length of the vectors of the document
                    ld = math.sqrt(sum([x**2 for x in freq.values()])) / max_freq
                else:
                    max_freq = 0
                    ld = 0

                InvertedIndexer.lock_documents_data.acquire()
                InvertedIndexer.documents_metadata[doc] = (ld, max_freq, title)
                InvertedIndexer.lock_documents_data.release()
            else:
                InvertedIndexer.lock_count.release()
                break

        print(threading.current_thread().__str__() + "has finished. Total docs processed: " + self.docs_processed.__str__())


# Updates the indexer in case of the desire to add any new data contained in data.npy
def update_indexer(number_of_threads=1):
    index_start_time = time.time()
    # Running the indexer in Threads
    indexer_threads = []
    for i in range(number_of_threads):
        indexer = InvertedIndexer()
        indexer.start()
        indexer_threads.append(indexer)
    for indexer in indexer_threads:
        indexer.join()
    index_end_time = time.time()
    # print(InvertedIndexer.indexer)
    # print(InvertedIndexer.documents_metadata)
    print("Time needed for indexer update: %f seconds" % (index_end_time-index_start_time))

    # Saving Inverted Indexer and documents' meta-data
    np.save(InvertedIndexer.INVERTED_INDEXER_FILENAME, InvertedIndexer.indexer)
    np.save(InvertedIndexer.DOC_METADATA_FILENAME, InvertedIndexer.documents_metadata)


# IN ORDER TO RUN ALONE TODO DELETE THIS
# InvertedIndexer.init_indexer(True)
# update_indexer(2)