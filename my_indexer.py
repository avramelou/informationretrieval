# -*- coding: utf-8 -*-
from Info_retrieval import metadata as meta
import numpy as np
import math
import threading
import time

import queue


class InvertedIndexer(threading.Thread):
    # Static-class variables
    indexer = None
    total_document_blocks = None
    document_blocks_read = 0
    documents_metadata = {}
    new_documents_found = 0
    document_queue = queue.Queue()

    # Locks for global(static) variables
    lock_indexer = threading.Lock()
    lock_documents_data = threading.Lock()
    lock_queue = threading.Lock()

    # Initialising indexer and it's static variables for the indexer
    @staticmethod
    def init_indexer(new_indexer=True):
        # First we will be loading the first block with documents/websites
        metadata_dict = np.load(meta.METADATA_DICTIONARY_FILE_PATH, allow_pickle=True).item()
        # Assert in order to confirm that this is a dictionary before the call of the get() operation
        assert isinstance(metadata_dict, dict)
        # Get the total amount of document blocks to read from
        InvertedIndexer.total_document_blocks = metadata_dict.get(meta.META_TOTAL_BLOCKS_KEY)

        # Variable that holds the boolean value that determines if a new inverted indexer will be made or not
        create_new_indexer = new_indexer

        # Check if we start over or we start from existed indexer
        if not create_new_indexer:
            try:
                InvertedIndexer.indexer = np.load(meta.INVERTED_INDEXER_FILE_PATH, allow_pickle=True).item()
                InvertedIndexer.documents_metadata = np.load(meta.DOC_METADATA_FILE_PATH, allow_pickle=True).item()
            except FileNotFoundError:
                print("File " + meta.INVERTED_INDEXER_FILE_PATH.__str__() +
                      " was not found. Starting inverted-indexer from scratch.")
                create_new_indexer = True

        if create_new_indexer:
            InvertedIndexer.indexer = {}

        # Returns True or False in order to make the inverted indexer from scratch again or not
        return create_new_indexer

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
            # Checks if the queue is empty or not in order to continue and get a document/website from it
            if InvertedIndexer.document_queue.empty():
                # If the queue is empty try to load another block of documents
                if InvertedIndexer.document_blocks_read != InvertedIndexer.total_document_blocks:
                    print("Loading block " + (InvertedIndexer.document_blocks_read + 1).__str__()
                          + " on indexer from disk")
                    next_block_file = (InvertedIndexer.document_blocks_read + 1).__str__() + ".npy"
                    pages_dictionary = np.load(meta.BLOCK_FILE_NAME_PREFIX_PATH + next_block_file, allow_pickle=True).item()
                    InvertedIndexer.document_blocks_read += 1
                    # Assert in order to confirm that this is a dictionary before the call of the get() operation
                    assert isinstance(pages_dictionary, dict)
                    # Re-filling the queue
                    for url, terms in pages_dictionary.items():
                        pair = (url, terms)
                        InvertedIndexer.document_queue.put(pair)
                    pages_dictionary.clear()
                else:
                    # If all the blocks of documents are read the indexer has been completed
                    InvertedIndexer.lock_queue.release()
                    break

            doc, (title, words) = InvertedIndexer.document_queue.get()
            InvertedIndexer.lock_queue.release()

            # Checks to find only unvisited documents
            # (in the case of running the algorithm for another time after the inverted dictionary was already made)
            InvertedIndexer.lock_documents_data.acquire()
            if doc in InvertedIndexer.documents_metadata.keys():
                InvertedIndexer.lock_documents_data.release()
                continue
            else:
                InvertedIndexer.new_documents_found += 1
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


# Updates the indexer in case of the desire to add any new data contained in data the data files
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
    print("Time needed for indexer update: %f seconds" % (index_end_time-index_start_time))
    print("New documents found since the last update: " + InvertedIndexer.new_documents_found.__str__())

    # Saving updated Inverted Indexer and documents' meta-data
    np.save(meta.INVERTED_INDEXER_FILE_PATH, InvertedIndexer.indexer)
    np.save(meta.DOC_METADATA_FILE_PATH, InvertedIndexer.documents_metadata)