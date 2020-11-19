# -*- coding: utf-8 -*-

import sys
import requests
import re
from bs4 import BeautifulSoup
import numpy as np
import threading
from collections import deque

# Function that removes the html tags, scripy CSS styling code
# from the text given html text, which basically is a string
def clean_html_text(html_text):
    # Create a new bs4 object from the html_text data loaded
    soup = BeautifulSoup(html_text, "html.parser")

    # Extract/Remove all script/Javascript and CSS styling code from the bs4 object
    for script in soup(["script", "style"]):
        script.extract()

    # Get the text from the bs4 object
    text = soup.get_text()

    # Break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())

    # Break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

    # Delete blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


# συναρτηση για το crawling
# start ειναι η σελιδα που αρχιζουμε
# num_of_links ειναι ο αριθμος των σελίδων που θα συλλέξουμε
# page_dict το dictionary που επιστρεφουμε με (link : κειμενο)
# search_algorithm είναι το string που εκφράζει τον τρόπο-αλγόριθμο που 
# θα γίνεται η αναζήτηση
def crawl(start, num_of_links, search_algorithm):
    # Using deque for both queue and stack operations
    # Deque is preferred over list in the cases where we need quicker append 
    # and pop operations from both the ends of container, as deque provides an 
    # O(1) time complexity for append and pop operations as compared to list which provides O(n) time complexity
    neighbors = deque() 

    # Dictionary that has the functions used for the different pop functions 
    # depending on the search algorithm to use
    queue_dict = {
      "DFS": neighbors.pop, # Works like LIFO
      "BFS": neighbors.popleft, # Works like FIFO
     }

    # Check that the search algorithm is listed in the queue_dict, in order to
    # work with the implemented crawl function. Otherwise throw an exception
    if not(search_algorithm in queue_dict):
      raise KeyError("No such algorithm as" + search_algorithm + "is supported")

    # Adding the starting website to the queue/deque used
    neighbors.append(start)

    # Define the dictionary that holds the links along with the extracted text
    page_dict = {}
    count = 0
    # Start the crawling
    while (count in range(0, num_of_links) and len(neighbors) > 0):
        neighbor = queue_dict[search_algorithm]()
        while (neighbor in page_dict):
            neighbor = queue_dict[search_algorithm]()
        page_text = requests.get(neighbor).text
        text = clean_html_text(page_text)
        page_dict.update({neighbor: text})

        # Adding the new links to the neighbors deque
        list_of_links = re.findall('(?<=<a href=")http[^"]*', page_text)
        # In case of using DFS adding in reserve-order(as visited-order) in the stack 
        if search_algorithm == "DFS":
          list_of_links.reverse()
        neighbors.extend(list_of_links)

        count += 1
    return page_dict


# Load args in variables
start = sys.argv[1]
num_of_links = int(sys.argv[2])
start_from_scratch = sys.argv[3]
threads = int(sys.argv[4])
algorithm = sys.argv[5]

# δημιουργουμε το page_dict αναλογα τον αλγοριθμο
page_dict = crawl(start, num_of_links,algorithm)

# Save dictionary in file
if (start_from_scratch == '1'):
    np.save("data.npy", page_dict)
else:
    old_save = np.load("data.npy", allow_pickle="True").item()
    page_dict.update(old_save)
    np.save("data.npy", page_dict)
    page = np.load("data.npy", allow_pickle="True").item()
