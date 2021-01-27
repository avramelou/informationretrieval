# -*- coding: utf-8 -*-
import numpy as np
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
    lock_queue = threading.Lock()

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
        else:
            for k, v in InvertedIndexer.document_dict.items():
                pair = (k, v)
                InvertedIndexer.document_queue.put(pair)

    @staticmethod
    def get_frequency_dict(term_list):
        # Creating an empty dictionary
        freq_dict = {}
        for item in term_list:
            if item in freq_dict:
                freq_dict[item] += 1
            else:
                freq_dict[item] = 1
        return freq_dict

    def __init__(self):
        threading.Thread.__init__(self)
        self.docs_processed = 0

    # Method that creates the indexer
    def run(self):
        print(threading.current_thread().__str__() + "has started" + '\n')
        # Thread sleep-delay before each of the thread's FIRST start only
        time.sleep(0.1)

        # Running until all documents are checked
        while True:
            InvertedIndexer.lock_queue.acquire()
            if InvertedIndexer.document_queue.empty() is True:
                InvertedIndexer.lock_queue.release()
                break

            else:
                doc, (title, words) = InvertedIndexer.document_queue.get()
                InvertedIndexer.lock_queue.release()

            # Checks to find only unvisited documents
            # (in the case of running the algorithm for another time after the inverted dictionary was already made)
            InvertedIndexer.lock_documents_data.acquire()
            if doc in InvertedIndexer.documents_metadata.keys():
                InvertedIndexer.lock_documents_data.release()
                continue
            InvertedIndexer.lock_documents_data.release()

            self.docs_processed += 1

            word_freq_dict = InvertedIndexer.get_frequency_dict(words)

            for word, word_count in word_freq_dict.items():
                InvertedIndexer.lock_indexer.acquire()
                # Word doesn't exist add it in the indexer { word : [1,[(doc,word_count)]]}
                if word not in InvertedIndexer.indexer:
                    # Update indexer with the new word
                    InvertedIndexer.indexer[word] = [1, [(doc, word_count)]]
                else:
                    # Get record for the word
                    values = InvertedIndexer.indexer.get(word)
                    InvertedIndexer.lock_indexer.release()
                    # Increase appearances in docs
                    values[0] += 1
                    # Append the document with it's word count
                    values[1].append((doc, word_count))
                    # Update indexer
                    InvertedIndexer.lock_indexer.acquire()
                    InvertedIndexer.indexer.update({word: values})  # Update indexer
                InvertedIndexer.lock_indexer.release()

            # Find the max_freq and Ld of document
            if len(word_freq_dict.values()) != 0:
                max_freq = max(word_freq_dict.values())
                #  The length of the vectors of the document
                ld = math.sqrt(sum([x ** 2 for x in word_freq_dict.values()])) / max_freq
            else:
                max_freq = 0
                ld = 0

            InvertedIndexer.lock_documents_data.acquire()
            InvertedIndexer.documents_metadata[doc] = (ld, max_freq, title)
            InvertedIndexer.lock_documents_data.release()

        print(threading.current_thread().__str__() + "has finished. Total docs processed: "
              + self.docs_processed.__str__())


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

    # todo change where this is
    # Saving Inverted Indexer and documents' meta-data
    np.save(InvertedIndexer.INVERTED_INDEXER_FILENAME, InvertedIndexer.indexer)
    np.save(InvertedIndexer.DOC_METADATA_FILENAME, InvertedIndexer.documents_metadata)


# IN ORDER TO RUN ALONE TODO DELETE THIS
# InvertedIndexer.init_indexer(True)
# update_indexer(1)