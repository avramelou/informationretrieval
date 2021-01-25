# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
from Info_retrieval.my_query_processor import *
from abc import ABC

app = Flask(__name__)


class FlaskApp(ABC):
    my_query_processor = QueryProcessor(new_indexer=False)
    given_query = None
    number_of_wanted_docs = None


@app.route('/')
def index():
    return render_template('index.html', urls={}, comments=" ", query="", topk="")


@app.route('/query', methods=['GET', 'POST'])
def get_query():
    FlaskApp.given_query = request.form['field']
    FlaskApp.number_of_wanted_docs = request.form['top_k']

    FlaskApp.number_of_wanted_docs = int(FlaskApp.number_of_wanted_docs) if (FlaskApp.number_of_wanted_docs != "") else 1

    top_k = FlaskApp.my_query_processor.top_k(FlaskApp.given_query, FlaskApp.number_of_wanted_docs)
    comments = ""

    if len(top_k) == 0:
        comments = "No results for this query! Please try again with other keywords"
    elif FlaskApp.number_of_wanted_docs > len(top_k):
        comments = "No more results to show"

    return render_template('index.html', urls=top_k.items(), comments=comments,
                           query=FlaskApp.given_query, topk=FlaskApp.number_of_wanted_docs)


@app.route('/feedback', methods=['GET', 'POST'])
def get_feedback():
    feedback = request.form.getlist('feedback')
    top_k_feedback = FlaskApp.my_query_processor.feedback(feedback, FlaskApp.number_of_wanted_docs)

    return render_template('index.html', urls=top_k_feedback.items(), comments="",
                           query=FlaskApp.given_query, topk=FlaskApp.number_of_wanted_docs)


app.run()
