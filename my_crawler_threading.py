# -*- coding: utf-8 -*-

import sys
import requests
import re
from bs4 import BeautifulSoup
import numpy as np
import threading
import more_itertools


# Function that removes the html tags, scripy CSS styling code
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

    # Getting the text - Removing HTML
    text = BeautifulSoup(html_text, "html.parser").get_text()

    # Removing non letters and numbers (which also removes css and js)
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


# (το foo μπορει να αλλαξει :P ) function for finding links in websites
# subneighbors is the sites in which we search for links
def foo(lock,subneighbors):
    while(len(subneighbors) > 0):
        neighbor = subneighbors[0]
        # thread takes the lock to check global variables
        lock.acquire()
        if(num_of_links <= len(page_dict)):
            lock.release()
            return
        while (neighbor in page_dict):
            subneighbors.pop(0)
            if(len(subneighbors) == 0):
                lock.release()
                return
        lock.release()
        
        # get text from html
        page_text = requests.get(neighbor).text
        text = clean_html_text(page_text)
    
        # Adding the new links to the neighbors list
        list_of_links = re.findall('(?<=<a href=")http[^"]*', page_text)
        
        # thead takes the lock in order to update global variables
        lock.acquire()
        page_dict.update({neighbor: text})
        neighbors.extend(list_of_links)
        lock.release()
        
        subneighbors.pop(0) 
    
        
        
    
    
    

# συναρτηση για το crawling
# start ειναι η σελιδα που αρχιζουμε
# threads ειναι ο αριθμός των theads
def crawl(start,threads):
    # get text from html
    page_text = requests.get(start).text
    text = clean_html_text(page_text)
    page_dict.update({start: text})
        
    # Adding the new links to the neighbors list
    list_of_links = re.findall('(?<=<a href=")http[^"]*', page_text)
    neighbors.extend(list_of_links)
    
    
    while (num_of_links > len(page_dict)):
        layer = neighbors.copy()
        neighbors.clear()
        step = len(layer) // threads + 1 #find how many sites every thread will take
        #create threads
        threads_list = []
        for i in range(0,threads):
            end = (i+1)*step  if (i+1)*step < len(layer) else len(layer)
            t = threading.Thread(target = foo , args=(lock,layer[i*step:end]))
            threads_list.append(t)
    
        #start threads
        for thread in threads_list:
            thread.start()
    
        #wait for all threads to end
        for thread in threads_list:
            thread.join()
    
    
        
   
# Load args in variables
start = sys.argv[1]
global num_of_links
num_of_links = int(sys.argv[2])
start_from_scratch = sys.argv[3]
threads = int(sys.argv[4])

# global variables
global neighbors
neighbors = [] # links in sites
global page_dict
page_dict = {} # sites that will be crawled
lock = threading.Lock()

# crawling
crawl(start, threads)

# keep only first nun_of_links elements of page dict
page_dict = dict(more_itertools.take(num_of_links,page_dict.items()))

# Save dictionary in file
if (start_from_scratch == '1'):
    np.save("data.npy", page_dict)
else:
    old_save = np.load("data.npy", allow_pickle="True").item()
    page_dict.update(old_save)
    np.save("data.npy", page_dict)
    page = np.load("data.npy", allow_pickle="True").item()
