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
from collections import defaultdict
import math

index_dir = "./index"
data_dir = "./Reddit_Data"

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

store = SimpleFSDirectory(Paths.get(index_dir))
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
writer = IndexWriter(store, config)

# Build link graph from Reddit posts
link_graph = defaultdict(set)
url_to_post = {}


def extract_links(post):
    links = set()
    if isinstance(post.get("body"), str):
        for word in post["body"].split():
            if word.startswith("http"):
                links.add(word.strip())
    for comment in post.get("comments", []):
        for word in comment.split():
            if word.startswith("http"):
                links.add(word.strip())
    return links


def compute_pagerank(graph, d=0.85, max_iter=50):
    scores = {node: 1.0 for node in graph}
    num_nodes = len(graph)
    for _ in range(max_iter):
        new_scores = {}
        for node in graph:
            rank_sum = 0
            for other in graph:
                if node in graph[other]:
                    rank_sum += scores[other] / len(graph[other])
            new_scores[node] = (1 - d) / num_nodes + d * rank_sum
        scores = new_scores
    return scores


def index_reddit(post, pr_scores):
    doc = Document()

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

    for comment in post.get("comments", []):
        doc.add(TextField("comment", comment, Field.Store.NO))

    url_titles = post.get("url_title")
    if url_titles and isinstance(url_titles, list):
        for url_title in url_titles:
            doc.add(TextField("url_title", url_title, Field.Store.YES))

    url = post.get("retrievedfrom")
    pr_score = pr_scores.get(url, 0.0)
    doc.add(FloatPoint("pagerank", pr_score))
    doc.add(StoredField("pagerank", pr_score))

    writer.addDocument(doc)


def crawled_links_index(post):
    doc = Document()
    title = post.get("title")
    if title:
        doc.add(TextField("link_title", title, Field.Store.YES))
    for text in post.get("body", []):
        doc.add(TextField("link_body", text, Field.Store.NO))
    doc.add(StringField("from_reddit", post.get("from_reddit", ""), Field.Store.YES))
    writer.addDocument(doc)


def main():
    print(lucene.VERSION)
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    reddit_data = sorted(glob(os.path.join(data_dir, "chunk_*.json")))
    reddit_data += sorted(glob(os.path.join(data_dir, "data_*.json")))

    all_posts = []
    for data in reddit_data:
        with open(data, 'r', encoding='utf-8') as f:
            submissions = json.load(f)
        for post in submissions:
            url = post.get("retrievedfrom")
            if url:
                url_to_post[url] = post
                out_links = extract_links(post)
                link_graph[url].update(out_links)
            all_posts.append(post)

    pagerank_scores = compute_pagerank(link_graph)

    for post in all_posts:
        index_reddit(post, pagerank_scores)

    crawled_data = sorted(glob(os.path.join(data_dir, "crawled_links_*.json")))
    for data in crawled_data:
        with open(data, 'r', encoding='utf-8') as f:
            links = json.load(f)
        for link in links:
            crawled_links_index(link)

    writer.close()


if __name__ == "__main__":
    main()

