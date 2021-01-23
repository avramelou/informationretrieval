from Info_retrieval import my_indexer_threading
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

    # indexer_update_threads parameter has no effect if parameter update_indexer_from_datafile is False
    def __init__(self, new_indexer=True, update_indexer_from_datafile=True, indexer_update_threads=1):
        my_indexer_threading.InvertedIndexer.init_indexer(new_indexer)

        self.query_vector = None
        self.accumulators = None

        if update_indexer_from_datafile:
            my_indexer_threading.update_indexer(number_of_threads=indexer_update_threads)

    def update_accumulator_for_term(self, term, tf_tq):
        # Searching term in the indexer
        term_indexer = my_indexer_threading.InvertedIndexer.indexer.get(term)

        # If the term exists in the indexer (in any documents)
        if term_indexer is not None:
            # The number of docs that contain the term
            n_t = term_indexer[0]
            total_docs = len(my_indexer_threading.InvertedIndexer.documents_metadata)
            idf_t = idf(total_docs, n_t)

            doc_indexer = term_indexer[1:]
            # Getting rid of the first item (which is the number of documents)
            for d_indexes in doc_indexer:
                # This part is the name/url of the document
                document = d_indexes[0]

                # The part which is the frequency list of that term in the current document
                freq_list = d_indexes[1:]

                freq_td = len(freq_list)  # The frequency of the term in the current document
                max_freq_doc = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[1]
                tf_td = tf(freq_td, max_freq_doc)

                # Updating accumulator
                doc_score = self.accumulators.get(document)
                if doc_score is not None:
                    updated_score = doc_score + (tf_td * idf_t) * tf_tq
                    self.accumulators[document] = updated_score
                else:
                    self.accumulators[document] = (tf_td * idf_t) * tf_tq

    def top_k(self, query, k=1):
        self.query_vector = {}

        # Initialise Accumulators
        self.accumulators = {}

        # The frequencies of the terms in the query
        terms_frequencies_dict = collections.Counter(query.split())
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
        print(self.accumulators)

        # Returning top k largest keys in the dictionary
        # Using heapq in order to return the top k documents in an efficient time of O(n log k)
        return heapq.nlargest(k, self.accumulators, key=self.accumulators.get)

    def top_k_feedback(self, k=1):
        # Initialise Accumulators
        self.accumulators = {}

        # For every term in query update the accumulator for that term
        for term in self.query_vector.keys():
            tf_tq = self.query_vector.get(term)
            self.update_accumulator_for_term(term, tf_tq)

        # List for irrelevant keys (documents) in order to delete them from the output
        irrelevant_keys = []

        # Updating similarities of accumulators based on the lengths
        for document in self.accumulators.keys():
            ld = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[0]
            similarity = self.accumulators.get(document)/ld

            # If the similarity shows that the specific document has a negative similarity value,
            # then mark it as irrelevant by adding the key to the irrelevant_keys list
            if similarity >= 0:
                self.accumulators.update({document: similarity})
            else:
                irrelevant_keys.append(document)

        # Delete irrelevant keys (documents) fom the accumulators
        for document in irrelevant_keys:
            del self.accumulators[document]

        # Returning top k largest keys in the dictionary
        # Using heapq in order to return the top k documents in an efficient time of O(n log k)
        return heapq.nlargest(k, self.accumulators, key=self.accumulators.get)

    def feedback(self, feedback_docs):
        relevant_docs = set(feedback_docs)
        non_relevant_docs = set()
        for document in self.accumulators.keys():
            if document not in relevant_docs:
                non_relevant_docs.add(document)

        indexer = my_indexer_threading.InvertedIndexer.indexer
        meta_data_docs = my_indexer_threading.InvertedIndexer.documents_metadata
        for term in indexer:
            values = indexer.get(term)

            # The number of docs that contain the term
            n_t = values[0]
            # The total docs
            total_docs = len(meta_data_docs)
            # Calculate idf for the term
            idf_t = idf(total_docs, n_t)
            # For every document that contains the term
            for i in range(1, len(values)):
                doc = values[i][0]

                if doc in relevant_docs:
                    freq_td = len(values)-1  # The frequency of the term in the current document
                    max_freq_doc = meta_data_docs.get(doc)[1]
                    tf_td = tf(freq_td, max_freq_doc)

                    # If the term already exists in the keys of the dictionary then take the previous value,
                    # else start from 0
                    prev_total = self.query_vector.get(term) if self.query_vector.get(term) is not None else 0
                    self.query_vector.update({term: prev_total + (0.5 * (idf_t * tf_td))/len(relevant_docs)})

                elif doc in non_relevant_docs:
                    freq_td = len(values)-1  # The frequency of the term in the current document
                    max_freq_doc = meta_data_docs.get(doc)[1]
                    tf_td = tf(freq_td, max_freq_doc)

                    # If the term already exists in the keys of the dictionary then take the previous value,
                    # else start from 0
                    prev_total = self.query_vector.get(term) if self.query_vector.get(term) is not None else 0
                    self.query_vector.update({term: prev_total - (0.25 * (idf_t * tf_td))/len(non_relevant_docs)})
        print(self.query_vector)
        return self.top_k_feedback(k=5)


def get_feedback():
    feedback_string = input("Give in a string the names of the documents that had best matched with the query (e.g. doc1 doc3): ")
    feedback_docs = feedback_string.split()

    print("selected documents: " + feedback_docs.__str__())
    return feedback_docs


my_query_processor = QueryProcessor(new_indexer=True, update_indexer_from_datafile=True, indexer_update_threads=1)
top_k = my_query_processor.top_k(query="Loukia Documents", k=10)
print("query vector: " + my_query_processor.query_vector.__str__())
print(top_k)

while True:
    print(my_query_processor.feedback(get_feedback()))
