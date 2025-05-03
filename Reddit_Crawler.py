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
import time
from queue import Queue
import urllib.robotparser
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup


reddit = praw.Reddit(client_id = "LT5IPPOzyPf63rNmKLBd0A",
                     client_secret = "44dm-FGppYj5Bu5354NEciMLalIwoA",
                     username = "Puzzleheaded_Buy5352",
                     password = "Agentx44smile!",
                     user_agent = "172Crawler")

BATCH_SIZE = 10
MAX_FILE_SIZE = 10
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
    i = 0
    while True:
        filename = f"{FILENAME}_{i}{EXT}"
        path = Path(DIRECTORY_NAME) / filename
        if not path.exists() or get_file_size(path) < MAX_FILE_SIZE:
            return path
        i += 1

def get_directory_size(directory):
    total_size = 0
    with lock2:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)
    return total_size / (1024 * 1024)

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
        # print("written")



#reddit crawling
#needs to check for dupes
def crawl(subreddit_name, sizeMB):
    print("Subreddit: {}".format(subreddit_name))
    subreddit = reddit.subreddit(subreddit_name)

    batch_data = []
    allSubmissions = {
            "hot": subreddit.hot(limit = None),
            "top": subreddit.top(time_filter="all", limit = None),
            "new": subreddit.new(limit = None)
    }

    retry_delay = 10


    for category, submissions in allSubmissions.items():
        for submission in submissions:
            try: 
                
                if get_directory_size("./Reddit_Data") >= sizeMB:
                    print("Size of Directory Exceeds Wanted Amount")
                    return
                post_data = {
                    "subreddit": subreddit_name,
                    "author": str(submission.author) if submission.author else "deleted",
                    "title": submission.title,
                    "URL": submission.url,
                    "permalink": submission.permalink,
                    "ID": submission.id,
                    "body": submission.selftext,
                    "upvotes": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "time": submission.created_utc,
                    "retrievedfrom": category,
                    "comments": []
                }

                #Scrape links, not sure if we need to scrape every single post though...
                #scrape_link({"link": submission.url})

                submission.comments.replace_more(limit=0)
                for top_level_comment in submission.comments[:20]:
                    post_data["comments"].append(top_level_comment.body)

                with lock1:
                    batch_data.append(post_data)
                
                if len(batch_data) >= BATCH_SIZE:
                    write_to_json(batch_data)
                    #print(f"Thread {threading.current_thread().name} wrote to JSON.")
                    batch_data = []

                time.sleep(1)

                retry_delay = 10

            except praw.exceptions.APIException as e:
                print(f"API Exception occurred: {e}")
                time.sleep(10)
            except prawcore.exceptions.TooManyRequests as e:
                print(f"{subreddit_name}'s Rate limit exceeded. Waiting for {retry_delay} seconds before retrying...")
                time.sleep(retry_delay)
                #exponential retry delay
                retry_delay = min(retry_delay * 2, 600)
            except prawcore.exceptions.ServerError as e:
                print(f"Server error occurred: {e}")
                time.sleep(10)
            except prawcore.exceptions.Forbidden as e:
                print(f"Fobidden access error: {e}")
                time.sleep(60)
            except Exception as e:
                print(f"Unexcepted error: {e}")
                time.sleep(10)
    if batch_data:
        write_to_json(batch_data)
        print("final print")

def is_reddit_link(url):
    parsed_url = urllib.parse.urlparse(url)
    return not (parsed_url.netlock.lower().endswith("reddit.com") or parsed_url.netlock.lower().endswith("redd.it"))
    
processed_links = set()

#looking for http links in jsons
def find_links(filename):
    path = Path(DIRECTORY_NAME) / filename
    links = Queue()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            #go through all data in json
            for submission in data:
                submission_id = submission.get("ID")
                url = submission.get("URL")
                body = submission.get("body")
                comments = submission.get("comments")
                #look for links in body and comment for links
                #check url if it isnt the same link to reddit
                #put into queue with info abt submission_id as well as received from url, body or comment
                #checks the url part of dict

                if not is_reddit_link(url) and url not in processed_links:
                    link_info = {
                        "url": url,
                        "submission_id": submission_id,
                        "filename": filename,
                        "from": "url"
                    }
                    processed_links.add(url) #we don't want to waste time and process the same link twice
                    links.put(link_info)
                
                #Check for links in body and comments
                if body:
                    found_links = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', body)
                    for link in found_links:
                        if link not in processed_links:
                            link_info = {
                                "link": link,
                                "submission_id": submission_id,
                                "from": "body"
                            }
                            processed_links.add(link)
                            links.put(link_info)

                # Check links in the comments
                for comment in comments:
                    found_links = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', comment)
                    for link in found_links:
                        if link not in processed_links:
                            link_info = {
                                "link": link,
                                "submission_id": submission_id,
                                "from": "comment"
                            }
                            processed_links.add(link)
                            links.put(link_info)

        except json.JSONDecodeError:
            print(f"Corrupted file: {path}")
            return 




def scrape_link(link_info):
    
    link = link_info['link']
    parse = urllib.parse.urlparse(link)
    robot_link = f"{parse.scheme}://{parse.netloc}/robots.txt"
    rb = urllib.robotparser.RobotFileParser(robot_link)
    rb.read()
    if not rb.can_fetch("*", link):
        print(f"Scraping not allowed for {link} according to robots.txt")
        return
        
    print("Scraping is allowed for this link")
    
    # Fetch and parse the webpage content
    try:
        response = urllib.request.urlopen(link)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract the title of the page
        title = soup.title.string if soup.title else "No title found"
        print(f"Title of the page: {title}")

        # Additional content extraction (example: article body)
        body = soup.find('div', class_='article-body') 
        if body:
            print(f"Article Body: {body.get_text()[:200]}...")  # Print the first 200 characters of the body
        
        # Example: save the link to a file (you can log it or add it to a database)
        with open("scraped_links.txt", "a") as file:
            file.write(link + "\n")
            print(f"Link saved: {link}")
        
    except Exception as e:
        print(f"Error scraping {link}: {e}")





subreddits = ['news', 'worldnews', 'worldpolitics', 'politics', 'newshub', 'newsandpolitics', 'futurology']
threads = []

def main():
    generate_directory(DIRECTORY_NAME)
    print(get_directory_size("./Reddit_Data"))
    try:
        for sub in subreddits:
            t = threading.Thread(target = crawl, args=(sub,400))
            t.daemon = True
            threads.append(t)
            t.start()

        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1)

    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        for t in threads:
            pass
        sys.exit(0)

if __name__ == "__main__":
    main()
