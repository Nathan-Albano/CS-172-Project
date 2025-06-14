## In the terminal, "export FLASK_APP=flask_demo" (without .py)
## flask run -h 0.0.0.0 -p 8888

import lucene
import os
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity
from flask import request, Flask, render_template

from java.util import HashMap
from java.lang import Float

import math
from datetime import datetime

app = Flask(__name__)

def retrieve(storedir, query):

    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))
    
    fields = ["title", "body", "comment", "url_title", "link_title", "link_body"]

    boosts = HashMap()
    boosts.put("title", Float(2.0))
    boosts.put("url_title", Float(2.0))
    boosts.put("link_title", Float(2.0))
    
    parser = MultiFieldQueryParser(fields, StandardAnalyzer(), boosts)
    parsed_query = MultiFieldQueryParser.parse(parser, str(query))

    topDocs = searcher.search(parsed_query, 10).scoreDocs
    topkdocs = []
    for hit in topDocs:
        doc = searcher.doc(hit.doc)
        print(doc.get("author"))
        print(doc.get("ratio"))
        topkdocs.append({
            "score": hit.score,
            "title": doc.get("title"),
            "subreddit": doc.get("subreddit"),
            "upvotes": doc.get("upvotes"),
            "time": doc.get("time"),
            "author": doc.get("author"),
            "ratio": doc.get("ratio"),
            "ID": doc.get("ID"),
            "pagerank": float(doc.get("pagerank") or 0.0)
        })
    return topkdocs
    #print(topkdocs)
def relevance_score(upvotes, downvotes, created_time_str):
    score = upvotes - downvotes
    sign = 1 if score > 0 else -1 if score < 0 else 0
    order = math.log(max(1, abs(score)))
    created_time = datetime.strptime(created_time_str, '%Y-%m-%d %H:%M:%S')
    beginning = datetime(1970,1,1)
    age = (created_time - beginning).total_seconds()
    age /= 45000
    return round(sign * order + age, 7)

@app.route("/")
def home():
    return render_template('input.html')

@app.route('/input', methods = ['POST', 'GET'])
def input():
    return render_template('input.html')

@app.route('/output', methods = ['POST', 'GET'])
def output():
    if request.method == 'GET':
        return f"Nothing"
    if request.method == 'POST':
        form_data = request.form
        query = form_data['query'].strip()
        if not query:
            return render_template('output.html', error="Please enter a query.")
        sort_by = form_data.get('filter_option', 'Relevant')
        #print(f"this is the query: {query}")
        lucene.getVMEnv().attachCurrentThread()
        docs = retrieve('index/', str(query))
        #print(docs)
        print(sort_by)
        for doc in docs:
            up = int(doc.get('upvotes'))
            down = (int(doc.get('upvotes')) - float(doc.get('ratio')) * int(doc.get('upvotes'))) / float(doc.get('ratio'))
            time = doc.get('time')
            doc['relevance_score'] = relevance_score(up, down, time)
        if sort_by == 'Lucene':
            pass
        elif sort_by == 'Relevant':
            docs.sort(key=lambda x: x.get('relevance_score'), reverse=True)

        elif sort_by == "Newest":
            docs.sort(key=lambda x: x.get('time'), reverse=True)
        elif sort_by == "Oldest":
            docs.sort(key=lambda x: x.get('time'))
        elif sort_by == "Most_Upvoted":
            docs.sort(key=lambda x: int(x.get('upvotes')), reverse=True)
        elif sort_by == "Controversial":
            docs.sort(key=lambda x: x.get('ratio'))
        elif sort_by == "PageRank":
            docs.sort(key=lambda x: x.get("pagerank", 0.0), reverse=True)

        return render_template('output.html',query_output = query,lucene_output = docs)
    
lucene.initVM(vmargs=['-Djava.awt.headless=true'])

    
if __name__ == "__main__":
    app.run(host='class-063.cs.ucr.edu', port=8888, debug=True)

# create_index('sample_lucene_index/')
# retrieve('sample_lucene_index/', 'web data')


