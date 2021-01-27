from Info_retrieval import my_indexer_threading
from Info_retrieval import my_text_processor
import numpy as np
import collections
import math
import heapq


def tf(freq_term, max_freq_term):
    return freq_term/max_freq_term
    # return 1 + np.log(freq_td)


def idf(total_docs, docs_with_term):
    return np.log(1 + (total_docs / docs_with_term))


class QueryProcessor:

    # Different weights constants for positive and negative feedback
    POS_FEEDBACK_W = 0.5
    NEG_FEEDBACK_W = -1 * 0.25

    # indexer_update_threads parameter has no effect if parameter update_indexer_from_datafile is False
    def __init__(self, new_indexer=True, update_indexer_from_datafile=True, indexer_update_threads=1):
        my_indexer_threading.InvertedIndexer.init_indexer(new_indexer)

        self.query_vector = None
        self.accumulators = None

        if update_indexer_from_datafile:
            my_indexer_threading.update_indexer(number_of_threads=indexer_update_threads)

    def update_accumulator_for_term(self, term, tf_tq):
        # The number of total documents
        total_docs = len(my_indexer_threading.InvertedIndexer.documents_metadata)

        # Searching term in the indexer
        term_indexer = my_indexer_threading.InvertedIndexer.indexer.get(term)

        # Structure of term_indexer: { term : [n_t, [(doc_0, term_count_in_doc_0), ... , (doc_n, term_count_in_doc_n)]]}

        # If the term exists in the indexer (in any documents)
        if term_indexer is not None:

            # The number of docs that contain the term
            n_t = term_indexer[0]

            # Calculate idf for the term
            idf_t = idf(total_docs, n_t)

            # This is the array with the items that have the document's url with
            # the frequency of the term in the current document
            doc_indexer = term_indexer[1]

            # For all the documents that contain this term/word,
            # Take name/url of the document
            # and the frequency of the term in the current document
            for document, freq_td in doc_indexer:

                # The max_frequency of among all the terms in that document
                max_freq_doc = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[1]

                # Calculating tf for the term
                tf_td = tf(freq_td, max_freq_doc)

                # Updating accumulator with the updated score of the tf-idf weight
                doc_score = self.accumulators.get(document)
                if doc_score is not None:
                    updated_score = doc_score + (tf_td * idf_t) * tf_tq
                    self.accumulators[document] = updated_score
                else:
                    self.accumulators[document] = (tf_td * idf_t) * tf_tq

    # Update the accumulators in order to keep only the top k documents,
    # return the top-k of the documents with their titles
    def get_top_k_documents_title_dict(self, k=1):

        # The top k largest keys in the dictionary of accumulators
        # Using heapq in order to return the top k documents in an efficient time of O(n log k)
        top_k_keys = heapq.nlargest(k, self.accumulators, key=self.accumulators.get)

        top_k_accumulator = {}
        top_k_documents_title_dict = {}

        # Adding the top_k documents with their similarities in a new dictionary,
        # also adding the top_k documents with their titles in a another dictionary that will be returned
        for document in top_k_keys:
            top_k_accumulator.update({document: self.accumulators.get(document)})
            doc_title = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[2]
            top_k_documents_title_dict.update({document: doc_title})

        # Update the accumulator to have only the top k documents
        self.accumulators = top_k_accumulator

        return top_k_documents_title_dict

    def top_k(self, query, k=1):
        self.query_vector = {}

        # Initialise Accumulators
        self.accumulators = {}

        # Applying the same pre-processing on the queries that was used in the crawler
        query_terms = my_text_processor.TextProcessor.get_useful_word_list(query, stemming=True)
        # The frequencies of the terms in the query
        terms_frequencies_dict = collections.Counter(query_terms)
        #  The frequency of the most frequent term in the query
        max_freq_query = max(terms_frequencies_dict.values())
        #  The length of the vectors of the query
        lq = math.sqrt(sum([x ** 2 for x in terms_frequencies_dict.values()])) / max_freq_query

        # For every term in query update the accumulator for that term
        for term in terms_frequencies_dict.keys():
            # Updating the query vector's weights (the tf of that term)
            freq_tq = terms_frequencies_dict.get(term)  # The frequency of this term in the query
            tf_tq = tf(freq_tq, max_freq_query)
            self.query_vector.update({term: tf_tq})

            # Update the documents' accumulator for that term
            self.update_accumulator_for_term(term, tf_tq)

        # Updating similarities of accumulators based on the lengths
        for document in self.accumulators.keys():
            ld = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[0]
            similarity = self.accumulators.get(document)/(ld*lq)
            self.accumulators.update({document: similarity})

        # Update the accumulators in order to keep only the top k documents,
        # return the top-k of the documents with their titles
        return self.get_top_k_documents_title_dict(k)

    def top_k_feedback(self, k=1):
        # Initialise Accumulators
        self.accumulators = {}

        # For every term in query update the accumulator for that term
        for term in self.query_vector.keys():
            tf_tq = self.query_vector.get(term)
            self.update_accumulator_for_term(term, tf_tq)

        # Updating similarities of accumulators based on the lengths
        for document in self.accumulators.keys():
            ld = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[0]
            similarity = self.accumulators.get(document)/ld
            self.accumulators.update({document: similarity})

        # Update the accumulators in order to keep only the top k documents,
        # return the top-k of the documents with their titles
        return self.get_top_k_documents_title_dict(k)

    def feedback(self, feedback_docs, k):
        # import time
        # start = time.time()
        relevant_docs = set(feedback_docs)
        non_relevant_docs = set()
        for document in self.accumulators.keys():
            if document not in relevant_docs:
                non_relevant_docs.add(document)

        meta_data_docs = my_indexer_threading.InvertedIndexer.documents_metadata

        # The number of total documents
        total_docs = len(meta_data_docs)

        # The inverted indexer with the terms as keys
        indexer = my_indexer_threading.InvertedIndexer.indexer

        for term in indexer:
            term_indexer = indexer.get(term)

            # The number of docs that contain the term
            n_t = term_indexer[0]

            # Calculate idf for the term
            idf_t = idf(total_docs, n_t)

            # This is the array with the items that have the document's url with
            # the frequency of the term in the current document
            doc_indexer = term_indexer[1]

            # For every document that contains the term make changes to the "query vector"
            for document, freq_td in doc_indexer:

                set_of_document = None
                weight = None

                if document in relevant_docs:
                    set_of_document = relevant_docs
                    # Since the document had positive feedback multiply with positive weight
                    weight = QueryProcessor.POS_FEEDBACK_W

                elif document in non_relevant_docs:
                    set_of_document = non_relevant_docs
                    # Since the document had negative feedback multiply with negative weight
                    weight = QueryProcessor.NEG_FEEDBACK_W

                if set_of_document is not None:
                    max_freq_doc = meta_data_docs.get(document)[1]
                    tf_td = tf(freq_td, max_freq_doc)

                    # If the term already exists in the keys of the dictionary then take the previous value,
                    # else start from 0
                    prev_total = self.query_vector.get(term)
                    if prev_total is None:
                        prev_total = 0

                    # Update the query vector
                    self.query_vector.update({term: prev_total + (weight * (idf_t * tf_td))/len(set_of_document)})

        # print("Total time for feedback: ", (time.time() - start))
        return self.top_k_feedback(k)

    # @staticmethod
    # def find_terms_in_feedback_docs(feedback_docs):
    #     terms = set()
    #
    #     for document in feedback_docs:
    #         (_title, doc_terms) = my_indexer_threading.InvertedIndexer.document_dict.get(document)
    #
    #         for term in doc_terms:
    #             terms.add(term)
    #     return terms

    # def feedback_new(self, positive_feedback_docs, k):
    #     import time
    #     start = time.time()
    #     relevant_docs = set(positive_feedback_docs)
    #     non_relevant_docs = set()
    #     for document in self.accumulators.keys():
    #         if document not in relevant_docs:
    #             non_relevant_docs.add(document)
    #
    #     meta_data_docs = my_indexer_threading.InvertedIndexer.documents_metadata
    #
    #     # The number of total documents
    #     total_docs = len(meta_data_docs)
    #
    #     print(self.accumulators.keys())  # TODO delete ths line
    #
    #     feedback_terms = QueryProcessor.find_terms_in_feedback_docs(self.accumulators.keys())
    #
    #     # The inverted indexer with the terms as keys
    #     indexer = my_indexer_threading.InvertedIndexer.indexer
    #
    #     for term in feedback_terms:
    #         term_indexer = indexer.get(term)
    #
    #         # The number of docs that contain the term
    #         n_t = term_indexer[0]
    #
    #         # Calculate idf for the term
    #         idf_t = idf(total_docs, n_t)
    #
    #         # This is the array with the items that have the document's url with
    #         # the frequency of the term in the current document
    #         doc_indexer = term_indexer[1]
    #
    #         # For every document that contains the term make changes to the "query vector"
    #         for document, freq_td in doc_indexer:
    #
    #             set_of_document = None
    #             weight = None
    #
    #             if document in relevant_docs:
    #                 set_of_document = relevant_docs
    #                 # Since the document had positive feedback multiply with positive weight
    #                 weight = QueryProcessor.POS_FEEDBACK_W
    #
    #             elif document in non_relevant_docs:
    #                 set_of_document = non_relevant_docs
    #                 # Since the document had negative feedback multiply with negative weight
    #                 weight = QueryProcessor.NEG_FEEDBACK_W
    #
    #             if set_of_document is not None:
    #                 max_freq_doc = meta_data_docs.get(document)[1]
    #                 tf_td = tf(freq_td, max_freq_doc)
    #
    #                 # If the term already exists in the keys of the dictionary then take the previous value,
    #                 # else start from 0
    #                 prev_total = self.query_vector.get(term)
    #                 if prev_total is None:
    #                     prev_total = 0
    #
    #                 # Update the query vector
    #                 self.query_vector.update({term: prev_total + (weight * (idf_t * tf_td))/len(set_of_document)})
    #
    #     print("feed back new time: ", (time.time() - start))
    #     return self.top_k_feedback(k)


