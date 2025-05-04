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
from collections import deque
import urllib.robotparser
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import requests



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

#gives next json. Each json stores 10MB at most
def get_latest_json(file=FILENAME):
    i = 0
    while True:
        filename = f"{file}_{i}{EXT}"
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

def write_to_json(entry, filename=FILENAME):
    with lock2:
        file_path = get_latest_json(filename)
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                print(f"Corrupted file: {file_path}")
                data = []
        else:
            data = []
        if isinstance(entry, list):
            data.extend(entry)
        else:
            data.append(entry)

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

def banned_link(url):
    if not url:
        return True
    return any(domain in url for domain in ["reddit.com", "redd.it", "imgur.com", "youtube.com", "youtu.be", "vimeo.com", "dailymotion.com", "twitch.tv", "facebook.com/watch"])

def extract_links(text):
    if not text:
        return []
    return re.findall(r'(https?://[^\s"\'>]+)', text)

#looking for http links in jsons
def find_links(directory):
    all_links = deque()
    seen_links = set()
    folder = Path(directory)

    for path in folder.glob("*.json"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            #go through all data in json
            for submission in data:
                filename = path.name

                submission_id = submission.get("ID")
                url = submission.get("URL", "")
                body = submission.get("body", "")
                comments = submission.get("comments", [])

                #checks the url 
                if url and not banned_link(url) and url not in seen_links:
                    all_links.append({
                        "url": url,
                        "submission_id": submission_id,
                        "filename": filename,
                        "from": "url",
                        "depth": 0
                    }) 
                    seen_links.add(url)
                
                #checks body
                body_links = extract_links(body)
                for link in body_links:
                    if not banned_link(link) and link not in seen_links:
                        all_links.append({
                            "url": link,
                            "submission_id": submission_id,
                            "filename": filename,
                            "from": "body",
                            "depth": 0
                        })
                        seen_links.add(link)
                #checks comments
                for comment in comments:
                    comment_links = extract_links(comment)
                    for link in comment_links:
                        if not banned_link(link) and link not in seen_links:
                            all_links.append({
                            "url": link,
                            "submission_id": submission_id,
                            "filename": filename,
                            "from": "comment",
                            "depth": 0
                            })
                            seen_links.add(link)
        except json.JSONDecodeError:
            print(f"Corrupted file: {path}")
            return 
    return all_links

def get_bodytext(text, min_words=5):
    lines = text.split('\n')
    body_text = [line.strip() for line in lines if len(line.strip().split()) > min_words]
    return body_text

def scrape_link(link_info, max_depth = 1):

    link = link_info['url']
    if link_info['depth'] > max_depth:
        return
    try:
        parse = urllib.parse.urlparse(link)
        robot_link = f"{parse.scheme}://{parse.netloc}/robots.txt"


        rb = urllib.robotparser.RobotFileParser() 
        allow_crawl = True 


        try:
            
            robots_response = requests.get(robot_link, timeout=10, headers={ 
                "User-Agent": "Mozilla/5.0 (compatible; Python RobotParser)"
            })

            if robots_response.status_code == 200:
              
                rb.parse(robots_response.text.splitlines())
                
                if not rb.can_fetch("*", link):
                    return 
                else:
                    allow_crawl = True 
            elif robots_response.status_code == 404:
                 allow_crawl = True 
            else:

                allow_crawl = False 
                return 

        except requests.RequestException as e:
            allow_crawl = True 


        if not allow_crawl:
             return




        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

        response = requests.get(link, headers=headers, timeout=15, allow_redirects=True)

        # ... (your existing parsing and saving logic) ...
        if response.status_code != 200:
            return

        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.title.string if soup.title else "No title found"
        for tag in soup(['script', 'style']):
            tag.decompose()

        body = soup.body
        if body:
            text = body.get_text(separator='\n', strip=True)
            body_text = get_bodytext(text)
            #print(body_text)
        else:
            body_text = ""
            #print("No body found")
        
        page_links = set()
        for anchor in soup.find_all('a', href=True):
            link_href = anchor['href']
            if link_href.startswith('http'):
                page_links.add(link_href)
            elif link_href.startswith('/'):
                page_links.add(parse.scheme + "://" + parse.netloc + link_href)
         
        
        data = {
            "URL": link,
            "from": link_info["from"],
            "title": title,
            "body": body_text,
            "from_reddit": link_info.get("submission_id") or "",
            "links": list(page_links), 
            "depth": link_info["depth"] + 1
        }

        write_to_json(data, "crawled_links")
        link_info_list = []
        for links in page_links:
            basic_link_info = {
                "url": links,
                "submission_id": link_info["submission_id"],
                "filename": get_latest_json("crawled_links"),
                "from": "crawled_url",
                "depth": link_info["depth"] + 1
            }
            link_info_list.append(basic_link_info)
        return link_info_list
        


    except requests.RequestException as e:
        # This catches errors during the MAIN page request
        #print("Network Error")
        #print(f"Network Error (requests.RequestException) scraping {link}: {e}")
        return
    except Exception as e:
        # Catch any other unexpected errors
       # print(f"General Error processing {link}: {e}")
        return 




subreddits = ['news', 'worldnews', 'worldpolitics', 'politics', 'newshub', 'newsandpolitics', 'futurology']

threads = []

def main():
    generate_directory(DIRECTORY_NAME)
    print(get_directory_size("./Reddit_Data"))
    # try:
    #     for sub in subreddits:
    #         t = threading.Thread(target = crawl, args=(sub,400))
    #         t.daemon = True
    #         threads.append(t)
    #         t.start()

    #     while any(t.is_alive() for t in threads):
    #         for t in threads:
    #             t.join(timeout=1)

    # except KeyboardInterrupt:
    #     print("Keyboard Interrupt")
    #     for t in threads:
    #         pass
    #     sys.exit(0)
    queue = find_links("./Reddit_Data_test/")
    while queue:

        more_links = scrape_link(queue[0])
        queue.popleft()
        if more_links:
            queue.extend(more_links)
    


if __name__ == "__main__":
    main()
