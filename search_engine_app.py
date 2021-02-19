# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
from my_query_processor import *
from abc import ABC

app = Flask(__name__)


class FlaskApp(ABC):
    my_query_processor = QueryProcessor(new_indexer=False)
    given_query = None
    number_of_wanted_docs = None


# index page of search engine
@app.route('/')
def index():
    return render_template('index.html', urls={}, comments=" ", query="", topk="")


# function for handling query
@app.route('/query', methods=['GET', 'POST'])
def get_query():
    #get query
    FlaskApp.given_query = request.form['field']
    # check if query is empty in order to not find results 
    if (FlaskApp.given_query.isspace() or not FlaskApp.given_query):
        return render_template('index.html', urls={}, comments=" ", query="", topk="")
    
    #get number of wanted documents 
    FlaskApp.number_of_wanted_docs = request.form['top_k']
    FlaskApp.number_of_wanted_docs = int(FlaskApp.number_of_wanted_docs) if (FlaskApp.number_of_wanted_docs != "") else 10

    #find top-k results
    top_k = FlaskApp.my_query_processor.top_k(FlaskApp.given_query, FlaskApp.number_of_wanted_docs)
    
    # comments for the user depending on results
    comments = ""
    if len(top_k) == 0:
        comments = "No results for this query! Please try again with other keywords"
    elif FlaskApp.number_of_wanted_docs > len(top_k):
        comments = "No more results to show"

    return render_template('index.html', urls=top_k.items(), comments=comments,
                           query=FlaskApp.given_query, topk=FlaskApp.number_of_wanted_docs)


# function for handling feedback
@app.route('/feedback', methods=['GET', 'POST'])
def get_feedback():
    #get relevant documents
    feedback = request.form.getlist('feedback')
    #get results after feedback
    top_k_feedback = FlaskApp.my_query_processor.feedback(feedback, FlaskApp.number_of_wanted_docs)

    return render_template('index.html', urls=top_k_feedback.items(), comments="",
                           query=FlaskApp.given_query, topk=FlaskApp.number_of_wanted_docs)


app.run()
