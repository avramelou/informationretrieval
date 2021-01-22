# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 12:36:14 2021

@author: User
"""

from flask import Flask, render_template, request
from informationretrieval.my_query_processor import *
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html', urls=enumerate({}),comments=" ")


@app.route('/query', methods=['GET', 'POST'])
def get_query():
    global query 
    query = request.form['field']
    global k 
    k = request.form['topk']
    
    k = int(k) if (k != "") else 1
    
    global my_query_processor 
    my_query_processor = QueryProcessor()
    global topk 
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
    
   
    return render_template('index.html',urls=enumerate(urls.items()),comments=comments)


@app.route('/feedback', methods=['GET', 'POST'])
def get_feedback():
    feedback = request.form.getlist('feedback')
       
    
    
    
    return "OK"
    
    
app.run()