from Info_retrieval import my_indexer_threading
import numpy as np
import collections
import math


def tf(freq_term, max_freq_term):
    return freq_term/max_freq_term
    # return 1 + np.log(freq_td)


def idf(total_docs, docs_with_term):
    return np.log(1 + (total_docs / docs_with_term))


class QueryProcessor:

    # indexer_update_threads parameter has no effect if parameter update_indexer_from_datafile is False
    def __init__(self, new_indexer=True, update_indexer_from_datafile=False, indexer_update_threads=1):
        my_indexer_threading.InvertedIndexer.init_indexer(new_indexer)

        self.query_vector = None
        self.accumulators = None

        if update_indexer_from_datafile:
            my_indexer_threading.update_indexer(number_of_threads=indexer_update_threads)

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

        # For every term in query
        for term in terms_frequencies_dict.keys():

            freq_tq = terms_frequencies_dict.get(term)  # The frequency of this term in the query
            tf_tq = tf(freq_tq, max_freq_query)

            self.query_vector.update({term: tf_tq})

            # Searching term in the indexer
            term_indexer = my_indexer_threading.InvertedIndexer.indexer.get(term)

            # If the term exists in the indexer (in any documents)
            if term_indexer is not None:
                # The number of docs that contain the term
                n_t = term_indexer[0]
                total_docs = len(my_indexer_threading.InvertedIndexer.documents_metadata)
                idf_t = idf(total_docs, n_t)

                doc_indexer = term_indexer
                # Getting rid of the first item (which is the number of documents)
                for d_indexes in doc_indexer[1:]:
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

        # Updating similarities of accumulators based on the lengths
        for document in self.accumulators.keys():
            ld = my_indexer_threading.InvertedIndexer.documents_metadata.get(document)[0]
            similarity = self.accumulators.get(document)/(ld*lq)
            self.accumulators.update({document: similarity})
        print(self.accumulators)

        import heapq
        # Returning top k largest keys in the dictionary
        # Using heapq in order to return the top k documents in an efficient time of O(n log k)
        return heapq.nlargest(k, self.accumulators, key=self.accumulators.get)

    def top_k_feedback(self, feedback_docs):
        relevant_docs = set(feedback_docs)
        non_relevant_docs = set()

        for document in self.accumulators.keys():
            if document not in relevant_docs:
                non_relevant_docs.add(document)

        # Load data from file
        document_dict = my_indexer_threading.InvertedIndexer.document_dict

        relevant_terms = set()
        # For relevant doc-words
        for doc in relevant_docs:
            relevant_terms.update(document_dict.get(doc).split())
            print(relevant_terms)


#        Not yet finished
def get_feedback():
    feedback_string = input("Give in a string the names of the documents that had best matched with the query (e.g. doc1 doc3): ")
    feedback_docs = feedback_string.split()

    print("selected documents: " + feedback_docs.__str__())
    return feedback_docs


my_query_processor = QueryProcessor(new_indexer=True, update_indexer_from_datafile=True, indexer_update_threads=1)
top_k = my_query_processor.top_k(query="Nick Nick Pedr", k=10)
print("query vector: " + my_query_processor.query_vector.__str__())
print(top_k)

my_query_processor.top_k_feedback(get_feedback())
