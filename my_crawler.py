# -*- coding: utf-8 -*-
from Info_retrieval import my_text_processor as my_tp
from Info_retrieval.my_text_processor import TextProcessor

import sys
import requests
import ssl
import threading
import time
import numpy as np
from collections import deque


class Crawler(threading.Thread):
    # Using deque for both queue and stack operations
    # Deque is preferred over list in the cases where we need quicker append
    # and pop operations from both the ends of container, as deque provides an
    # O(1) time complexity for append and pop operations as compared to list which provides O(n) time complexity
    crawling_links = deque()

    page_dict = {}  # The  dictionary with the sites and words that have been crawled
    num_of_links_to_crawl = None
    num_of_good_links_crawled = 0
    get_link_from_search_set = None  # The method's signature of the deque that will be called to get a new link

    # Dictionary that has the functions used for the different pop operations
    # depending on the search algorithm that will be used
    queue_dict = {
        "DFS": crawling_links.pop,  # Works like LIFO
        "BFS": crawling_links.popleft,  # Works like FIFO
    }

    # Since the crawler class uses it's objects/instances as a way to crawl different websites with many threads,
    # locks(semaphores) are going to be used. Using static locks for the Crawler so that
    # two threads won't be able to access the queue or the dictionary at same time.
    # This lock is used for the read/write of the crawling_links deque
    crawling_links_lock = threading.Lock()
    # This lock is used for the read/write of the dictionary and the num_of_good_links_crawled
    dictionary_lock = threading.Lock()

    # The TextProcessor object that will use stemming
    # and will also be used for other text processing/manipulation operations
    text_processor = TextProcessor(stemming=True)

    @staticmethod
    def init_static_variables(start_url, num_of_links_to_crawl, search_algorithm):
        Crawler.crawling_links.append(start_url)
        Crawler.num_of_links_to_crawl = num_of_links_to_crawl

        # Check that the search algorithm is listed in the queue_dict, in order to
        # work with the implemented crawl function. Otherwise throw an exception
        if not (search_algorithm in Crawler.queue_dict):
            raise KeyError("No such algorithm as" + search_algorithm + "is supported")
        else:
            Crawler.get_link_from_search_set = Crawler.queue_dict[search_algorithm]

    # Constructor of the class, initialize the thread
    def __init__(self, start_url):
        threading.Thread.__init__(self)
        self.starting_url = start_url
        self.num_of_links_crawled_by_self = 0

    @staticmethod
    def remove_bad_links_from_dict():
        # Cleaning null terms
        cleaned_page_dict = {}
        for k, v in Crawler.page_dict.items():
            if v is not None:
                cleaned_page_dict[k] = v
        Crawler.page_dict = cleaned_page_dict

    # TODO Not used, can be deleted
    # @staticmethod
    # def remove_extra_links_from_dict():
    #     # In case of having more links saved inKeeping only first num_of_links elements of page dict
    #     Crawler.page_dict = dict(more_itertools.take(Crawler.num_of_links_to_crawl, Crawler.page_dict.items()))

    # Performs a GET-request for the header only in order to avoid the crawling on this website
    # if it has non-text content type, or it's length is tremendously big
    @staticmethod
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

    # The run method start the crawling on this Crawler's object(thread)
    def run(self):
        print(threading.current_thread().__str__() + "has started crawling" + '\n')
        # Thread sleep-delay before each of the thread's FIRST start
        time.sleep(0.1)

        # Crawling while the number of good links crawled are less than the desired links
        while True:
            Crawler.dictionary_lock.acquire()
            if Crawler.num_of_good_links_crawled >= Crawler.num_of_links_to_crawl:
                Crawler.dictionary_lock.release()
                break

            # If the dequeue (the set of the links that the crawler is searching)
            # is currently empty, free the locks and skip this loop until another thread adds links to the set
            Crawler.crawling_links_lock.acquire()
            if len(Crawler.crawling_links) == 0:
                Crawler.dictionary_lock.release()
                Crawler.crawling_links_lock.release()
                continue

            link = Crawler.get_link_from_search_set()
            Crawler.crawling_links_lock.release()

            # If this link has already been crawled free the locks and
            # skip this loop in order to find an alternative website
            if link in Crawler.page_dict:
                Crawler.dictionary_lock.release()
                continue

            # Reserving the link (the key of the dictionary) in order to avoid collision with other threads
            # Reserving / marked as visited (the pages with None keys will be deleted later)
            Crawler.page_dict.update({link: None})
            Crawler.dictionary_lock.release()

            # Increasing self(thread's) num of links checked
            self.num_of_links_crawled_by_self += 1

            # Trying to crawl on the website
            # If crawling on that specific website is failed, we are not deleting from the dictionary
            # In order to not to crawl again on that specific website (The the empty content links are deleter later on)
            try:
                # First, checking for the header only, in order to avoid crawling on this website
                # if it has non-text content type, or it's length is tremendously big
                Crawler.check_endpoint_header(link)
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
            # Only saving the website if the the number of good links crawled are less than the desired links
            if Crawler.num_of_good_links_crawled < Crawler.num_of_links_to_crawl:
                # Updating the dictionary with the content
                Crawler.page_dict.update({link: (title, cleaned_words)})
                # Increasing the number of good links crawled by 1
                Crawler.num_of_good_links_crawled += 1
                print("Kept website Num " + Crawler.num_of_good_links_crawled.__str__() + ": " + link
                      + " from: " + threading.current_thread().__str__() + '\n')
            Crawler.dictionary_lock.release()

            # Finding the new links that start with "https" only
            list_of_links = TextProcessor.get_https_links_from_text(html_text)

            # If the search algorithm is DFS then reserve the order of the list in order to
            # get the expected search order in the search-set
            if Crawler.get_link_from_search_set == Crawler.crawling_links.pop:
                list_of_links.reverse()

            # Adding only the new links (the ones that don't exist in the dictionary set)
            for new_link in list_of_links:
                Crawler.crawling_links_lock.acquire()
                if new_link not in Crawler.page_dict:  # and (urlparse(link).netloc == "www.python.org"):
                    Crawler.crawling_links.append(new_link)
                Crawler.crawling_links_lock.release()

        print(threading.current_thread().__str__() + " has finished. Thread checked "
              + self.num_of_links_crawled_by_self.__str__() + " websites" + '\n')


# Loading args in variables
starting_url = sys.argv[1]
num_of_links = int(sys.argv[2])
start_from_scratch = sys.argv[3]
number_of_threads = int(sys.argv[4])
algorithm = sys.argv[5]

# Global variables
crawler_threads = []

Crawler.init_static_variables(start_url=starting_url,
                              num_of_links_to_crawl=num_of_links, search_algorithm=algorithm)

crawl_start_time = time.time()
for i in range(number_of_threads):
    # Notice that all the threads start from the same starting url, but only one will crawl this website
    crawler = Crawler(start_url=starting_url)
    crawler.start()
    crawler_threads.append(crawler)
for crawler in crawler_threads:
    crawler.join()
print("Crawling time: %s seconds" % format((time.time() - crawl_start_time), ".2f"))
print("Total Number of web-pages checked or visited were: " + len(Crawler.page_dict).__str__())
# Removing bad web-pages (the ones that an error or exception was occurred)
Crawler.remove_bad_links_from_dict()
print("Total Number of web-pages kept are: " + len(Crawler.page_dict).__str__())


# Save dictionary in file
if start_from_scratch == '1':
    np.save("data.npy", Crawler.page_dict)
else:
    old_save = np.load("data.npy", allow_pickle=True).item()
    Crawler.page_dict.update(old_save)
    np.save("data.npy", Crawler.page_dict)
    page = np.load("data.npy", allow_pickle=True).item()