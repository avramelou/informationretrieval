# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 18:13:15 2020

@author: User
"""

import sys
import requests
import re
from bs4 import BeautifulSoup
import numpy as np
import threading

# συναρτηση για dfs searching
# start ειναι η σελιδα που αρχιζουμε
# num_of_links ειναι ο αριθμος των σελίδων που θα συλλέξουμε
# page_dict το dictionary που επιστρεφουμε με (link : κειμενο)
def dfs_searching(start,num_of_links):
    count = 1
    page_dict = {} 
    neighbors = [] # σελιδες που συλλεγουμε 
    
    page_text = requests.get(start).text #html text σελιδας
    soup = BeautifulSoup(page_text,features="lxml")
    for script in soup(["script", "style"]): # remove all javascript and stylesheet code
        script.extract()
    text = soup.get_text()
    page_dict.update({start : text}) 
    neighbors = re.findall('(?<=<a href=")http[^"]*',page_text) + neighbors # βρισκουμε τα links της σελιδας
   
    while(count in range(1,num_of_links) and len(neighbors)>0):
        #ελεγχος αν επισκεφτηκαμε τη σελιδα ήδη 
        while(neighbors[0] in page_dict ):
            neighbors.pop(0)
        page_text = requests.get(neighbors[0]).text
        soup = BeautifulSoup(page_text,features="lxml")
        for script in soup(["script", "style"]): # remove all javascript and stylesheet code
            script.extract()
        text = soup.get_text()
        page_dict.update({neighbors[0] : text})
        neighbors = re.findall('(?<=<a href=")http[^"]*',page_text) + neighbors
        
        neighbors.pop(0)
        count+=1
    
    return page_dict


# συναρτηση για bfs searching
# start ειναι η σελιδα που αρχιζουμε
# num_of_links ειναι ο αριθμος των σελίδων που θα συλλέξουμε
# page_dict το dictionary που επιστρεφουμε με (link : κειμενο)
# (τα ιδια ακριβως με dfs απλα τωρα βαζουμε τα links στο τελος της λιστας)
def bfs_searching(start,num_of_links):
    count = 1
    page_dict = {}
    neighbors = []

    page_text = requests.get(start).text
    soup = BeautifulSoup(page_text,features="lxml")
    for script in soup(["script", "style"]): # remove all javascript and stylesheet code
        script.extract()
    text = soup.get_text()
    page_dict.update({start : text})
    neighbors+= re.findall('(?<=<a href=")http[^"]*',page_text)
    
    while(count in range(0,num_of_links) and len(neighbors)>0):
        while(neighbors[0] in page_dict ): 
            neighbors.pop(0)
        page_text = requests.get(neighbors[0]).text
        soup = BeautifulSoup(page_text,features="lxml")
        for script in soup(["script", "style"]): # remove all javascript and stylesheet code
            script.extract()
        text = soup.get_text()
        page_dict.update({neighbors[0] : text})
        neighbors+= re.findall('(?<=<a href=")http[^"]*',page_text)
        
        neighbors.pop(0)
        count+=1
    
    return page_dict



#load args in variables
start = sys.argv[1]
num_of_links = int(sys.argv[2])
start_from_scratch = sys.argv[3]
threads = int(sys.argv[4])
algorithm = sys.argv[5]


# δημιουργουμε το page_dict αναλογα τον αλγοριθμο
page_dict = {}
if(algorithm == "DFS"):
    page_dict = dfs_searching(start,num_of_links)
elif(algorithm == "BFS"):
    page_dict = bfs_searching(start,num_of_links)
else:
    print("ΚΑΠΟΙΟΣ ΑΛΛΟΣ ΑΛΓΟΡΙΘΜΟΣ")


#save dictionary in file 
if(start_from_scratch == '1'):
    np.save("data.npy",page_dict)
else:
    old_save = np.load("data.npy",allow_pickle="True").item()
    page_dict.update(old_save)
    np.save("data.npy",page_dict)
    page = np.load("data.npy",allow_pickle="True").item()


    





















