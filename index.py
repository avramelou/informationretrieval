# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 12:36:14 2021

@author: User
"""

from flask import Flask, render_template, request
from my_query_processor import *
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html', urls={},comments=" ",query="",topk="")


@app.route('/query', methods=['GET', 'POST'])
def get_query():
    global query  
    query = request.form['field']
    global k 
    k = request.form['topk']
    
    k = int(k) if (k != "") else 1
    
    global my_query_processor 
    my_query_processor = QueryProcessor(new_indexer=False)
    topk = my_query_processor.top_k(query,k)
    
    urls = {}
    for x in topk:
        soup = BeautifulSoup(requests.get(x).text,'html.parser')
        urls[x] = soup.title.text
        
   
    comments=""
    if len(topk) == 0:
        comments = "No results for this query! Please try again with other keywords"
    elif (k>len(topk)):
        comments = "No more results to show"
    
   
    return render_template('index.html',urls=urls.items(),comments=comments,query=query,topk=k)


@app.route('/feedback', methods=['GET', 'POST'])
def get_feedback():
    feedback = request.form.getlist('feedback')
    
    feedback_response = my_query_processor.feedback(feedback,k)
    
    urls = {}
    for x in feedback_response:
        soup = BeautifulSoup(requests.get(x).text,'html.parser')
        urls[x] = soup.title.text
    
    return render_template('index.html',urls=urls.items(),comments="",query=query,topk=k)

 
global query
global k   
app.run()