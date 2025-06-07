import lucene
import os
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StringField, IntPoint, FloatPoint, StoredField
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity
from flask import request, Flask, render_template

from glob import glob
from datetime import datetime
import json 
index_dir = "./index"
data_dir = "./Reddit_Data"

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

store = SimpleFSDirectory(Paths.get(index_dir))
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
writer = IndexWriter(store, config)


def index_reddit(post, crawled = None):
    doc = Document()

    #stored not tokenized
    stringFieldType = FieldType()
    stringFieldType.setStored(True)
    stringFieldType.setTokenized(False)

    #tokenized and stored
    textFieldType = FieldType()
    textFieldType.setStored(True)
    textFieldType.setTokenized(True)
    

    doc.add(StringField("subreddit", post.get("subreddit"), Field.Store.YES))
    doc.add(StringField("author", post.get("author"), Field.Store.YES))
    doc.add(StringField("from", post.get("retrievedfrom"), Field.Store.YES))
    doc.add(StringField("ID", post.get("ID"), Field.Store.YES))

    doc.add(IntPoint("upvotes", int(post.get("upvotes"))))
    doc.add(StoredField("upvotes", int(post.get("upvotes"))))

    doc.add(FloatPoint("ratio", post.get("upvote_ratio")))
    doc.add(StoredField("ratio", post.get("upvote_ratio")))
    
    unix_time = int(post.get("time"))
    time_str = datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
    doc.add(StringField("time", time_str, Field.Store.YES))

    doc.add(TextField("title", post.get("title"), Field.Store.YES))
    doc.add(TextField("body", post.get("body"), Field.Store.NO))

    comments = post.get("comments")
    for comment in comments:
        doc.add(TextField("comment", comment, Field.Store.NO))

    url_titles = post.get("url_title")
    if url_titles and isinstance(url_titles,list):
        for url_title in url_titles:
            doc.add(TextField("url_title", url_title, Field.Store.YES))

    if crawled:
        for link in crawled:
            title = link.get("title")
            if title:
                doc.add(TextField("link_title", title, Field.Store.YES))
            for text in link.get("body", []):
                doc.add(TextField("link_body", text, Field.Store.NO))

    writer.addDocument(doc)

# def crawled_links_index(post):
#     doc = Document()
#     title = post.get("title")
#     if title:
#         doc.add(TextField("link_title", title, Field.Store.YES))


#     body = post.get("body")
#     for text in body:
#         doc.add(TextField("link_body", text, Field.Store.NO))
#     doc.add(StringField("from_reddit", post.get("from_reddit", ""), Field.Store.YES))

#     writer.addDocument(doc)


def main():
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
    reddit_data = sorted(glob(os.path.join(data_dir, "chunk_*.json")))
    reddit_data += sorted(glob(os.path.join(data_dir, "data_*.json")))
    crawled_data = sorted(glob(os.path.join(data_dir, "crawled_links_*.json")))

    crawled_map = {}
    for file in crawled_data:
        with open(file, 'r', encoding='utf-8') as f:
            links = json.load(f)
        for link in links:
            reddit_id = link.get("from_reddit")
            if reddit_id:
                if reddit_id not in crawled_map:
                    crawled_map[reddit_id] = []
                crawled_map[reddit_id].append(link)


    for data in reddit_data:
        with open(data, 'r', encoding='utf-8') as f:
            submissions = json.load(f)
        for submission in submissions:
            reddit_id = submission.get("ID")
            crawled = crawled_map.get(reddit_id)
            if crawled:
                index_reddit(submission, crawled)
            else:
                index_reddit(submission)

    writer.close()

if __name__ == "__main__":
    main()
