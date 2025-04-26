#Reddit Crawler
import praw
import json
import threading
import os
import datetime
from praw.models import MoreComments
from pathlib import Path
import sys
import re
import prawcore.exceptions


reddit = praw.Reddit(client_id = "LT5IPPOzyPf63rNmKLBd0A",
                     client_secret = "44dm-FGppYj5Bu5354NEciMLalIwoA",
                     username = "Puzzleheaded_Buy5352",
                     password = "Agentx44smile!",
                     user_agent = "172Crawler")


BATCH_SIZE = 10
MAX_FILE_SIZE = 50
DIRECTORY_NAME = './Reddit_Data'
FILENAME = 'data'
EXT = '.json'

#data directory
def generate_directory(DIRECTORY_NAME):
    if not os.path.exists(DIRECTORY_NAME):
        os.mkdir(DIRECTORY_NAME)

#check size of json 
def get_file_size(file):
    return os.path.getsize(file) / (1024 * 1024)

#gives next json. Each json stores 50MB at most
def get_latest_json(FILENAME):
    i = 1
    while True:
        filename = f"{FILENAME}_{i}{EXT}"
        path = Path(DIRECTORY_NAME) / filename
        if not path.exists() or get_file_size(path) < MAX_FILE_SIZE:
            return path
        i += 1

#function for duplicates
# def checkDupes(directory):

#for race condition
lock1 = threading.Lock()
lock2 = threading.Lock()

def write_to_json(batch_data):
    with lock2:
        file_path = get_latest_json(FILENAME)
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                print(f"Corrupted file: {file_path}")
                data = []
        else:
            data = []

        data.extend(batch_data)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("written")



#reddit crawling
#needs to check for dupes
def crawl(subreddit_name):
    print("Subreddit: {}".format(subreddit_name))
    subreddit = reddit.subreddit(subreddit_name)

    batch_data = []
    allSubmissions = {
            "hot": subreddit.hot(limit = None),
            "top": subreddit.top(limit = None),
            "new": subreddit.new(limit = None)
        }
    with lock1:    
        for category, submissions in allSubmissions.items():
            for submission in submissions:
                post_data = {
                    "subreddit": subreddit_name,
                    "author": str(submission.author) if submission.author else "deleted",
                    "title": submission.title,
                    "URL": submission.url,
                    "ID": submission.id,
                    "body": submission.selftext,
                    "upvotes": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "visited": submission.visited,
                    "time": submission.created_utc,
                    "retrievedfrom": category,
                    "comments": []
                }
                submission.comments.replace_more(limit=0)
                for top_level_comment in submission.comments[:10]:
                    post_data["comments"].append(top_level_comment.body)
                print(post_data)
                batch_data.append(post_data)
                
                if len(batch_data) >= BATCH_SIZE:
                    write_to_json(batch_data)
                    print("written to json")
                    batch_data = []
            if batch_data:
                write_to_json(batch_data)


#looking for http links in jsons
# def lookForLinks(directory):




subreddits = ['genshin_impact_leaks', 'honkaistarrail_leaks', 'wutheringwavesleaks']
threads = []

def main():
    generate_directory(DIRECTORY_NAME)
    try:
        for sub in subreddits:
            t = threading.Thread(target = crawl, args=(sub,))
            t.daemon = True
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        sys.exit(0)

if __name__ == "__main__":
    main()
