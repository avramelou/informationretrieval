# -*- coding: utf-8 -*-

import sys
import requests
import re
import threading
import more_itertools
import time
import numpy as np
from bs4 import BeautifulSoup
from collections import deque


class Crawler(threading.Thread):
    # Using deque for both queue and stack operations
    # Deque is preferred over list in the cases where we need quicker append
    # and pop operations from both the ends of container, as deque provides an
    # O(1) time complexity for append and pop operations as compared to list which provides O(n) time complexity
    crawling_links = deque()

    # Using static  locks for the Crawler so that two threads won't be able to access
    # the queue or the dictionary at same time
    crawling_links_lock = threading.Lock()
    dictionary_lock = threading.Lock()

    page_dict = {}  # The  dictionary with the sites and words that have been crawled
    num_of_links_to_crawl = None
    get_link = None  # The function/method's signature of the deque that will be called to get a new link

    # Dictionary that has the functions used for the different pop functions
    # depending on the search algorithm that will be used
    queue_dict = {
        "DFS": crawling_links.pop,  # Works like LIFO
        "BFS": crawling_links.popleft,  # Works like FIFO
    }

    @staticmethod
    def init_static_variables(start_url, num_of_links_to_crawl, search_algorithm):
        Crawler.crawling_links.append(start_url)
        Crawler.num_of_links_to_crawl = num_of_links_to_crawl

        # Check that the search algorithm is listed in the queue_dict, in order to
        # work with the implemented crawl function. Otherwise throw an exception
        if not (search_algorithm in Crawler.queue_dict):
            raise KeyError("No such algorithm as" + search_algorithm + "is supported")
        else:
            Crawler.get_link = Crawler.queue_dict[search_algorithm]

    @staticmethod
    def remove_bad_from_dict():
        # Cleaning null terms
        clean = {}
        for k, v in Crawler.page_dict.items():
            if v is not None:
                clean[k] = v
        Crawler.page_dict = clean

        # Keeping only first nun_of_links elements of page dict
        Crawler.page_dict = dict(more_itertools.take(Crawler.num_of_links_to_crawl, Crawler.page_dict.items()))

    def __init__(self, start_url):
        threading.Thread.__init__(self)
        self.starting_url = start_url

    def run(self):
        print(threading.current_thread().__str__() + "has started crawling on: " + self.starting_url)

        while len(Crawler.page_dict) < Crawler.num_of_links_to_crawl:

            Crawler.crawling_links_lock.acquire()

            # If the dequeue (the set of the links that the crawler is searching)
            # is currently empty, skip this loop
            if len(Crawler.crawling_links) == 0:
                Crawler.crawling_links_lock.release()
                continue

            link = Crawler.get_link()

            # Reserving the link (the key of the dictionary) in order to avoid collision with other threads
            Crawler.dictionary_lock.acquire()
            while link in Crawler.page_dict:
                link = Crawler.get_link()
            # Reserving / marked as visited (the pages with None keys will be deleted later)
            Crawler.page_dict.update({link: None})
            print(len(Crawler.page_dict).__str__() + ": " + link)
            Crawler.dictionary_lock.release()

            Crawler.crawling_links_lock.release()

            # Trying to crawl on the website
            # If crawling on that specific website is failed, we are not deleting from the dictionary
            # In order to not to crawl again on that specific website
            try:
                # Checks if the content_type is not text or unknown in order to avoid the crawling on that website
                content_type = requests.head(link).headers.get('Content-Type')
                if content_type is None:
                    raise requests.exceptions.MissingSchema("Website Content error: " + "None Content Type")
                elif not content_type.startswith('text/'):
                    raise requests.exceptions.InvalidSchema("Website Content error: " + content_type)
                else:
                    # Get request, raises a timeout if 7 seconds are passed without response from the server
                    page_text = requests.get(link, timeout=(7, 7)).text

            except (requests.exceptions.Timeout, requests.exceptions.MissingSchema, requests.exceptions.ConnectionError,
                    requests.exceptions.InvalidURL, requests.exceptions.InvalidSchema, requests.exceptions.TooManyRedirects) as e:
                print(e.__str__() + " on website: " + link)
                continue

            cleaned_words = clean_html_text(page_text, stemming=False)

            # Updating the dictionary with the content
            Crawler.dictionary_lock.acquire()
            Crawler.page_dict.update({link: cleaned_words})
            Crawler.dictionary_lock.release()

            # Finding the new links that start with "https" # TODO: Change to http
            list_of_links = re.findall('(?<=<a href=")https[^"]*', page_text)

            # If the search algorithm is DFS then reserve the order of the list in order to
            # get the expected search order in the search-set
            if Crawler.get_link == Crawler.crawling_links.pop:
                list_of_links.reverse()

            # Adding only the new links (the ones that don't exist in the dictionary set)
            for new_link in list_of_links:
                Crawler.dictionary_lock.acquire()
                if new_link not in Crawler.page_dict:  # and (urlparse(link).netloc == "www.python.org"):
                    Crawler.crawling_links.append(new_link)
                Crawler.dictionary_lock.release()

        print(threading.current_thread().__str__())


# Function that removes the html tags, scripts, CSS styling code
# from the text given html text, which basically is a string
def clean_html_text_old(html_text):
    # Create a new bs4 object from the html_text data loaded
    soup = BeautifulSoup(html_text, "html.parser")

    # Extract/Remove all script/Javascript and CSS styling code from the bs4 object
    for script in soup(["script", "style"]):
        script.extract()

    # Get the text from the bs4 object
    text = soup.get_text()

    # Keep only english characters and words
    # text = re.sub("[^a-zA-Z0-9 \n]+", "", text)

    # Break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())

    # Break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

    # Delete blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


def clean_html_text(html_text, stemming=False):
    # Downloading the stop words
    # import nltk
    # nltk.download('stopwords')

    # # Using nltk to get the list of stop words
    # # Importing the stop word list from nltk
    # from nltk.corpus import stopwords

    # The list of stop words
    stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll",
                  "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's",
                  'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs',
                  'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those',
                  'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
                  'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
                  'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before',
                  'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under',
                  'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any',
                  'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own',
                  'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should',
                  "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
                  "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven',
                  "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan',
                  "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn',
                  "wouldn't"]

    # Searching in a set is a lot faster than searching in a list
    # Converting the stop words to a set
    stop_words = set(stop_words)

    # Getting the html file
    soup = BeautifulSoup(html_text, "html.parser")

    # Extract/Remove all script/Javascript and CSS styling code from the bs4 object
    for script in soup(["script", "style"]):
        script.extract()

    # Get the text from the bs4 object
    text = soup.get_text()

    # Removing non letters and numbers
    letters_only = re.sub("[^a-zA-Z]", " ", text)

    # Converting to lower case, split into individual words
    words = letters_only.lower().split()

    # Removing stop words, in order to keep only the useful words
    useful_words = [word for word in words if word not in stop_words]

    if stemming:
        # Stemming words
        # Stemming in order to break a word down into its' root
        from nltk.stem.snowball import SnowballStemmer
        stemmer = SnowballStemmer('english')
        useful_words = [stemmer.stem(word) for word in useful_words]

    # Joining the words back into one string separated by new line
    return "\n".join(useful_words)


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
    crawler = Crawler(start_url=starting_url)
    crawler.start()
    crawler_threads.append(crawler)
for crawler in crawler_threads:
    crawler.join()
print("Crawling time: %s seconds" % format((time.time() - crawl_start_time), ".2f"))
print("Total Number of web-pages visited are: " + len(Crawler.page_dict).__str__())
# Removing bad web-pages (the ones with None words or that the connection occurred with an error/exception)
Crawler.remove_bad_from_dict()
print("Total Number of web-pages kept are: " + len(Crawler.page_dict).__str__())


# Save dictionary in file
if start_from_scratch == '1':
    np.save("data.npy", Crawler.page_dict)
else:
    old_save = np.load("data.npy", allow_pickle=True).item()
    Crawler.page_dict.update(old_save)
    np.save("data.npy", Crawler.page_dict)
    page = np.load("data.npy", allow_pickle=True).item()