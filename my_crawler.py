# -*- coding: utf-8 -*-
from Info_retrieval import my_text_processor as my_tp
from Info_retrieval.my_text_processor import TextProcessor
from Info_retrieval import metadata as meta

import sys
import requests
import ssl
import threading
import time
import numpy as np
from collections import deque


class Crawler(threading.Thread):
    # Dictionary that holds all the metadata
    metadata_dict = None
    # The total number of website blocks saved in the files
    total_blocks = None
    # Each block will have a specific number of websites saved in it
    max_block_size = None
    # Set that holds the links that have been crawled
    crawled_links_set = None

    # Using deque for both queue and stack operations
    # Deque is preferred over list in the cases where we need quicker append
    # and pop operations from both the ends of container, as deque provides an
    # O(1) time complexity for append and pop operations as compared to list which provides O(n) time complexity
    crawling_links_deque = deque()

    pages_dictionary = None  # The  dictionary with the websites and words that have been crawled
    num_of_links_to_crawl = None
    links_crawled_in_session = 0  # Set that holds the number of links that have been crawled only in this session
    kept_links_crawled_in_session = 0
    count_consecutive_empty_queue_accesses = 0

    # The method's signature of the deque that will be called to get a new link
    get_link_from_search_set = None

    # Dictionary that has the functions used for the different pop operations
    # depending on the search algorithm that will be used
    queue_dict = {
        "DFS": crawling_links_deque.pop,  # Works like LIFO
        "BFS": crawling_links_deque.popleft,  # Works like FIFO
    }

    # Since the crawler class uses it's objects/instances as a way to crawl different websites with many threads,
    # locks(semaphores) are going to be used. Using static locks for the Crawler so that
    # two threads won't be able to access the queue or the dictionary at same time.
    # This lock is used for the read/write of the crawling_links_deque deque
    crawling_links_deque_lock = threading.Lock()
    # This lock is used for the read/write of the dictionary and the kept_links_crawled_in_session
    dictionary_lock = threading.Lock()

    # The TextProcessor object that will use stemming
    # and will also be used for other text processing/manipulation operations
    text_processor = TextProcessor(stemming=True)

    @staticmethod
    def init_metadata_dict():
        # Dictionary that holds all the metadata
        Crawler.metadata_dict = {}
        # The default total number of website blocks saved in the files
        Crawler.total_blocks = 0
        # Each block will have by default some websites saved in it
        Crawler.max_block_size = meta.DEFAULT_MAX_BLOCK_SIZE
        # Set that holds the links that have been crawled
        Crawler.crawled_links_set = set()

    @staticmethod
    def init_static_variables(start_url, num_of_links_to_crawl, search_algorithm, start_from_scratch=1, number_of_threads=1):
        # Check that the search algorithm is listed in the queue_dict, in order to
        # work with the implemented crawl function. Otherwise throw an exception
        if not (search_algorithm in Crawler.queue_dict):
            raise KeyError("No such algorithm as" + search_algorithm + "is supported")
        else:
            Crawler.get_link_from_search_set = Crawler.queue_dict[search_algorithm]

        if start_from_scratch:
            Crawler.init_metadata_dict()
            # The current block's dictionary with the websites and words that have been crawled
            Crawler.pages_dictionary = {}
        else:
            # todo check the warning of get
            try:
                Crawler.metadata_dict = np.load(meta.METADATA_DICTIONARY_FILE_PATH, allow_pickle=True).item()
                # Assert in order to confirm that this is a dictionary before the call of the get() operation
                assert isinstance(Crawler.metadata_dict, dict)
                Crawler.total_blocks = Crawler.metadata_dict.get(meta.META_TOTAL_BLOCKS_KEY)
                Crawler.max_block_size = Crawler.metadata_dict.get(meta.META_BLOCK_SIZE_KEY)
                Crawler.crawled_links_set = Crawler.metadata_dict.get(meta.META_CRAWLED_LINKS_SET_KEY)

                # The current block's dictionary with the websites and words that have been crawled
                Crawler.pages_dictionary = np.load(meta.BLOCK_FILE_NAME_PREFIX_PATH + Crawler.total_blocks.__str__() +
                                                   ".npy", allow_pickle=True).item()
                # Assert in order to confirm that this is a dictionary before the call of the get() operation
                assert isinstance(Crawler.pages_dictionary, dict)
                # Checks if the current block is "full" in order to make a new one
                if len(Crawler.pages_dictionary) == Crawler.max_block_size:
                    Crawler.pages_dictionary = {}
            except FileNotFoundError:
                print("File " + meta.METADATA_DICTIONARY_FILE_PATH.__str__() +
                      " was not found. Starting crawling from scratch.")
                Crawler.init_metadata_dict()
                # The current block's dictionary with the websites and words that have been crawled
                Crawler.pages_dictionary = {}

        Crawler.num_of_links_to_crawl = num_of_links_to_crawl
        Crawler.number_of_threads = number_of_threads
        Crawler.crawling_links_deque.append(start_url)

        # If the starting url is already visited then we might not be able to add more new documents
        if start_url in Crawler.crawled_links_set:
            return False
        else:
            return True

    @staticmethod
    def clear_dictionary_and_save_block_to_disk():
        Crawler.total_blocks += 1
        block_file_name = meta.BLOCK_FILE_NAME_PREFIX_PATH + Crawler.total_blocks.__str__() + ".npy"
        np.save(block_file_name, Crawler.pages_dictionary)
        Crawler.pages_dictionary.clear()
        Crawler.pages_dictionary = {}

    @staticmethod
    def save_last_page_dict():
        if len(Crawler.pages_dictionary) > 0:
            Crawler.clear_dictionary_and_save_block_to_disk()

    @staticmethod
    def update_metadata_file():
        Crawler.metadata_dict.update({meta.META_TOTAL_BLOCKS_KEY: Crawler.total_blocks})
        Crawler.metadata_dict.update({meta.META_CRAWLED_LINKS_SET_KEY: Crawler.crawled_links_set})
        np.save(meta.METADATA_DICTIONARY_FILE_PATH, Crawler.metadata_dict)

    @staticmethod
    def add_website_content_to_current_block(link, title, cleaned_words):
        # Only saving the website if the the number of good links crawled are less than the desired links
        if Crawler.kept_links_crawled_in_session < Crawler.num_of_links_to_crawl:
            # Updating the dictionary with the content
            Crawler.pages_dictionary.update({link: (title, cleaned_words)})
            Crawler.kept_links_crawled_in_session += 1
            print("Kept website Num " + Crawler.kept_links_crawled_in_session.__str__() + ": " + link
                  + " from: " + threading.current_thread().__str__() + '\n')
            # Checks if the current block is "full" in order to save it to the disk and clear it from the memory
            if len(Crawler.pages_dictionary) == Crawler.max_block_size:
                Crawler.clear_dictionary_and_save_block_to_disk()

    # Constructor of the class, initialize the thread
    def __init__(self, start_url):
        threading.Thread.__init__(self)
        self.starting_url = start_url
        self.num_of_links_crawled_by_self = 0

    # The run method start the crawling on this Crawler's object(thread)
    def run(self):
        print(threading.current_thread().__str__() + "has started crawling" + '\n')
        # Thread sleep-delay before each of the thread's FIRST start
        time.sleep(0.1)

        # Crawling while the number of good links crawled are less than the desired links
        while True:
            Crawler.dictionary_lock.acquire()
            if Crawler.kept_links_crawled_in_session >= Crawler.num_of_links_to_crawl:
                Crawler.dictionary_lock.release()
                break

            # If the dequeue (the set of the links that the crawler is searching)
            # is currently empty, free the locks and skip this loop until another thread adds links to the set
            Crawler.crawling_links_deque_lock.acquire()
            if len(Crawler.crawling_links_deque) == 0:
                Crawler.dictionary_lock.release()
                Crawler.crawling_links_deque_lock.release()

                # Thread sleep-delay to allow time to another thread to add to the crawling_links_deque
                time.sleep(0.5)
                Crawler.count_consecutive_empty_queue_accesses += 1
                # Allowing an upper bound of maximum consecutive times that the queue will be empty for the threads
                if Crawler.count_consecutive_empty_queue_accesses >= number_of_threads * 2:
                    print(threading.current_thread().__str__() + " is released, "
                                                                 "too many times tried to access empty queue. "
                                                                 "No out-links.")
                    break
                continue
            else:
                Crawler.count_consecutive_empty_queue_accesses = 0

            link = Crawler.get_link_from_search_set()
            Crawler.crawling_links_deque_lock.release()

            # If this link has already been crawled free the locks and
            # skip this loop in order to find an alternative website
            if link in Crawler.crawled_links_set:
                Crawler.dictionary_lock.release()
                continue

            #  Reserving / marked as visited, in order to avoid collision with other threads
            Crawler.crawled_links_set.add(link)
            Crawler.dictionary_lock.release()

            # Increasing number of links checked
            Crawler.links_crawled_in_session += 1
            # Increasing self(thread's) number of links checked
            self.num_of_links_crawled_by_self += 1

            # Trying to crawl on the website
            # If crawling on that specific website is failed, we are not deleting from the dictionary
            # In order to not to crawl again on that specific website (The the empty content links are deleter later on)
            try:
                # First, checking for the header only, in order to avoid crawling on this website
                # if it has non-text content type, or it's length is tremendously big
                check_endpoint_header(link)
                # If no exception/error was raised so far,
                # Commit a get-request, raises a timeout if 5 seconds are passed without response from the server
                html_text = requests.get(link, timeout=(5, 5), allow_redirects=True).text
            except(Exception, ssl.SSLWantReadError, requests.exceptions.Timeout, requests.exceptions.MissingSchema,
                   requests.exceptions.ConnectionError, requests.exceptions.InvalidURL,
                   requests.exceptions.InvalidSchema, requests.exceptions.TooManyRedirects) as e:
                print("Website " + link + " was not saved: " + e.__str__() + '\n')
                continue  # Continue crawling in other websites

            # Try to clean the text and keep only the useful words in a list.
            # If most of the words of this text are not in the English vocabulary,
            # an exception will be caught in order to avoid saving the text of that website
            try:
                # Getting the title along with the cleaned words
                title, cleaned_words = Crawler.text_processor.get_cleaned_doc(html_text)
            except (Exception, my_tp.lang_detect_exception.LangDetectException) as e:
                print("Website " + link + " was not saved: " + e.__str__() + '\n')
                continue  # Continue crawling in other websites

            # If the website does not have a title, the one used is the original link
            if title is None:
                title = link

            Crawler.dictionary_lock.acquire()
            Crawler.add_website_content_to_current_block(link, title, cleaned_words)
            Crawler.dictionary_lock.release()

            # Finding the new links that start with "https" only
            list_of_links = TextProcessor.get_https_links_from_text(html_text)

            # If the search algorithm is DFS then reserve the order of the list in order to
            # get the expected search order in the search-set
            if Crawler.get_link_from_search_set == Crawler.crawling_links_deque.pop:
                list_of_links.reverse()

            # Adding only the new links (the ones that don't exist in the dictionary set)
            for new_link in list_of_links:
                Crawler.crawling_links_deque_lock.acquire()
                if new_link not in Crawler.crawled_links_set:
                    Crawler.crawling_links_deque.append(new_link)
                Crawler.crawling_links_deque_lock.release()

        print(threading.current_thread().__str__() + " has finished. Thread checked "
              + self.num_of_links_crawled_by_self.__str__() + " websites" + '\n')


# Performs a GET-request for the header only in order to avoid the crawling on this website
# if it has non-text content type, or it's length is tremendously big
def check_endpoint_header(url):
    # Get request for the header only and the important thing is the setting allow_redirects=True,
    # in order to get the final website's header in case of redirection.
    # Raises a timeout if 5 seconds are passed without response from the server
    head_response = requests.head(url, timeout=(5, 5), allow_redirects=True)
    header = head_response.headers

    # Checks if the content-type is not text or unknown in order to avoid the crawling on this website
    content_type = header.get('content-type')
    if content_type is None:
        raise requests.exceptions.MissingSchema("Website Content error: " + "None Content Type")
    elif not content_type.startswith('text/'):
        raise requests.exceptions.InvalidSchema("Website Content error - Wrong content type: " + content_type)

    # Checks if the content-length is not big or unknown in order to avoid the crawling on this website
    content_length = header.get('content-length', None)
    if content_length is None:
        raise requests.exceptions.MissingSchema("Website Content error: " + "None Content Length")
    elif float(content_length) > 2e7:
        raise requests.exceptions.InvalidSchema("Website Content error: Size of " + content_length + " is too big")


# Loading args in variables

start = time.time()

starting_url = sys.argv[1]
num_of_links = int(sys.argv[2])
start_from_scratch = sys.argv[3]
number_of_threads = int(sys.argv[4])
algorithm = sys.argv[5]

if start_from_scratch == '1':
    start_from_scratch = True
else:
    start_from_scratch = False

# Global variables
crawler_threads = []

starting_url_not_visited = Crawler.init_static_variables(start_url=starting_url, num_of_links_to_crawl=num_of_links,
                                                         start_from_scratch=start_from_scratch, search_algorithm=algorithm,
                                                         number_of_threads=number_of_threads)

if starting_url_not_visited:
    crawl_start_time = time.time()
    for i in range(number_of_threads):
        # Notice that all the threads start from the same starting url, but only one will crawl this website
        crawler = Crawler(start_url=starting_url)
        crawler.start()
        crawler_threads.append(crawler)
    for crawler in crawler_threads:
        crawler.join()

    print("Crawling time: %s seconds" % format((time.time() - crawl_start_time), ".2f"))
    print("Total Number of web-pages checked or visited in this session were: " + Crawler.links_crawled_in_session.__str__())
    print("Number of web-pages kept in this session are: " + Crawler.kept_links_crawled_in_session.__str__())
    print("Total time to crawl: ", (time.time() - start))

    Crawler.save_last_page_dict()
    Crawler.update_metadata_file()
else:
    print(starting_url + " is already visited. You can run the crawler again "
                         "and give a non-visited website to start from.")


