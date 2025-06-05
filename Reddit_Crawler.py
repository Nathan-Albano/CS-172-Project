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
import argparse

#If we want to do a check for duplicates between runs
SEEN_IDS_FILE = 'seen_submission_ids.json'
CRAWLED_LINKS_FILE = 'crawled_links.json'
BATCH_SIZE = 10
MAX_FILE_SIZE = 10
DIRECTORY_NAME = './Reddit_Data_Chunks - Copy'
FILENAME = 'chunk'
EXT = '.json'

#for race condition
lock1 = threading.Lock()
lock2 = threading.Lock()

#added for crawler reddit crawler that each thread checks a set() to see if its a duplicate
lock3 = threading.Lock() 
seen_submission_ids = set()

lock4 = threading.Lock()
crawled_links = set()

def parse_args():
    parser = argparse.ArgumentParser(description="Reddit Crawler")
    parser.add_argument('--subreddits', type=str, required=True, help="Comma separated list of subreddit names")
    parser.add_argument('--sizeMB', type=int, required=True, help="Size of the data to scrape in MB")
    parser.add_argument('--outputDir', type=str, required=True, help="Directory to store scraped data")
    return parser.parse_args()

#loading in previous seen ids
def load_submission_ids():
    if os.path.exists(SEEN_IDS_FILE):
        try:
            with open(SEEN_IDS_FILE, 'r', encoding='utf-8') as f:
                ids = json.load(f)
            with lock3:
                seen_submission_ids.update(ids)
            print(f"Loaded {len(ids)} seen submission IDs from {SEEN_IDS_FILE}")
        except Exception as e:
            print(f"Error loading seen submission IDs: {e}")

# loading already crawled links
def load_crawled_links():
    if os.path.exists(CRAWLED_LINKS_FILE):
        try:
            with open(CRAWLED_LINKS_FILE, 'r', encoding='utf-8') as f:
                urls = json.load(f)
            with lock4:
                crawled_links.update(urls)
            print(f"Loaded {len(urls)} seen submission IDs from {SEEN_IDS_FILE}")
        except Exception as e:
            print(f"Error loading seen submission IDs: {e}")

#saving seen ids 
def save_submission_ids():
    with lock3:
        with open(SEEN_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(seen_submission_ids), f)
        print(f"Saved {len(seen_submission_ids)} seen submission IDs to {SEEN_IDS_FILE}")

#saving seen links
def save_crawled_links():
    with lock4:
        with open(SEEN_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(crawled_links), f)
        print(f"Saved {len(crawled_links)} seen submission IDs to {SEEN_IDS_FILE}")

#checks for previously already retrieved reddit submission ids
def check_prev_reddit_jsons(filename=FILENAME, directory=DIRECTORY_NAME):
    max_num = get_latest_json_num(filename)
    folder = Path(directory)
    for i in range(max_num):
        file_path = folder / f"{filename}_{i}.json"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                datas = json.load(f)
            new_datas = []
            for data in datas:
                id = data.get("ID")
                if id not in seen_submission_ids:
                    seen_submission_ids.add(id)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Invalid JSON: {file_path}")    

#checks for previously already crawled links
def check_prev_crawled_links_jsons(filename="crawled_links", directory=DIRECTORY_NAME):
    max_num = get_latest_json_num(filename)
    print(max_num)
    folder = Path(directory)
    for i in range(max_num):
        file_path = folder / f"{filename}_{i}.json"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                datas = json.load(f)
            new_datas = []
            for data in datas:
                url = data.get("URL")
                if url not in crawled_links:
                    crawled_links.add(url)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Invalid JSON: {file_path}")       


# Initialize Reddit API
reddit = praw.Reddit(client_id = "sr-sDM8dej6x24LRwASyXA",
                    client_secret = "GPtPevpMImC9yMbhhPJ_THgC0QWRHg",
                    username = "Clear_Market_7033",
                    password = "Proninjamonkey123",
                    user_agent = "turibl")

#data directory
def generate_directory(DIRECTORY_NAME):
    if not os.path.exists(DIRECTORY_NAME):
        os.mkdir(DIRECTORY_NAME)

#check size of json 
def get_file_size(file):
    return os.path.getsize(file) / (1024 * 1024)

#load json file not multi threaded
def load_json_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)
    
#save json file not multi threaded
def save_json_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

#for linking links to original submissions
def get_all_crawled_info(file, directory):
    max_num = get_latest_json_num(file)
    all_info = []
    print(max_num)
    for i in range(max_num + 1):
        filepath = Path(directory) / f"{file}_{i}.json"
        all_info.extend(load_json_file(filepath))
    return all_info

#for updating the reddit posts with link titles
def update_chunks(chunk, file, directory):
    max_num = get_latest_json_num(chunk)
    print(f"Final Instance of file: {max_num}")
    crawled_info = get_all_crawled_info(file, directory)
    for i in range(max_num + 1):
        filepath = Path(directory) / f"{chunk}_{i}.json"
        chunk_info = load_json_file(filepath)
        for object in chunk_info:
            sid = object.get("ID")
            matches = [entry for entry in crawled_info if entry.get("from_reddit") == sid]

            url_titles = [m.get("title") for m in matches if m.get("from") == "url" and m.get("title")]
            body_links = [(m.get("URL"), m.get("title")) for m in matches if m.get("from") == "body" and m.get("URL") and m.get("title")]
            comments_links = [(m.get("URL"), m.get("title")) for m in matches if m.get("from") == "comment" and m.get("URL") and m.get("title")]

            object["url_title"] = url_titles
            object["body_links"] = body_links
            object["comments_links"] = comments_links
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(chunk_info, f, indent = 4)

#gives next json. Each json stores 10MB at most
def get_latest_json(file=FILENAME):
    i = 0
    while True:
        filename = f"{file}_{i}"
        path = Path(DIRECTORY_NAME) / filename
        if not path.exists() or get_file_size(path) < MAX_FILE_SIZE:
            return path
        i += 1

def get_latest_json_num(file=FILENAME):
    i = 0
    while True:
        filename = f"{file}_{i}"
        path = Path(DIRECTORY_NAME) / filename
        if not path.exists() or get_file_size(path) < MAX_FILE_SIZE:
            return i
        try:
            with open(path, 'r', encoding='utf-8') as f:
                json.load(f)
        except json.JSONDecodeError:
            print(f"[Corrupt JSON] {filename} is invalid — skipping.")
            return i  
        i += 1

def get_directory_size(directory):
    total_size = 0
    with lock2:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)
    return total_size / (1024 * 1024) 



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
                with lock3:
                    if submission.id in seen_submission_ids:
                        print(f"Submission {submission.id} already seen. Skipping...")
                        continue
                    else:
                        print(f"Processing submission {submission.id}...")
                        seen_submission_ids.add(submission.id)

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

def find_links_in_file(file, directory=DIRECTORY_NAME):
    all_links = deque()
    folder = Path(directory)
    path = folder / file
    seen_links = set()
    seen_links.update(crawled_links)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for submission in data:
            filename = path.name
            submission_id = submission.get("ID")
            url = submission.get("URL", "")
            body = submission.get("body", "")
            comments = submission.get("comments", [])

            # Check the URL
            if url and not banned_link(url) and url not in seen_links:
                all_links.append({
                    "url": url,
                    "submission_id": submission_id,
                    "filename": filename,
                    "from": "url",
                    "depth": 0
                })
                seen_links.add(url)

            # Check body
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

            # Check comments
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
    with lock4:
        crawled_links.add(link)
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
        for p in soup.find_all('p'):
            for anchor in p.find_all('a', href=True):
                link_href = anchor['href']
                if link_href.startswith('http'):
                    page_links.add(link_href)
                elif link_href.startswith('/'):
                    full_link = f"{parse.scheme}://{parse.netloc}{link_href}"
                    page_links.add(full_link)
         
        
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

def crawl_links_in_queue(number, directory=DIRECTORY_NAME):
    queue = find_links_in_file(f"{FILENAME}_{number}.json", directory=DIRECTORY_NAME)
    print(len(queue))
    if not queue:
        return
    
    while queue:
        link_info = queue[0]
        scrape_link(link_info)
        queue.popleft()
    print(f"Thread-{number} done link crawling")

subreddits = ['news', 'worldnews', 'worldpolitics', 'politics', 'newshub', 'newsandpolitics', 'futurology']

threads1 = []
threads2 = []

def load_graph(directory=DIRECTORY_NAME, file="crawled_links"):
    """Load the link graph from crawled_links JSON files."""
    from collections import defaultdict
    max_num = get_latest_json_num(file, directory)
    graph = defaultdict(set)
    all_urls = set()
    for i in range(max_num + 1):
        path = Path(directory) / f"{file}_{i}.json"
        if not path.exists():
            continue
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for entry in data:
                src = entry.get("URL")
                links = entry.get("links", [])
                if src:
                    all_urls.add(src)
                    for dst in links:
                        graph[src].add(dst)
                        all_urls.add(dst)
        except Exception as e:
            print(f"Error loading {path}: {e}")
    for url in all_urls:
        if url not in graph:
            graph[url] = set()
    return graph

def pagerank(graph, damping=0.85, max_iter=100, tol=1e-6):
    """Compute PageRank scores for the given graph."""
    N = len(graph)
    ranks = {node: 1.0 / N for node in graph}
    for iteration in range(max_iter):
        new_ranks = {}
        for node in graph:
            rank_sum = 0.0
            for src in graph:
                if node in graph[src]:
                    rank_sum += ranks[src] / (len(graph[src]) if graph[src] else N)
            new_ranks[node] = (1 - damping) / N + damping * rank_sum
        diff = sum(abs(new_ranks[n] - ranks[n]) for n in graph)
        ranks = new_ranks
        if diff < tol:
            print(f"Converged after {iteration+1} iterations.")
            break
    return ranks

def run_pagerank():
    """Run PageRank on crawled links and print top results."""
    graph = load_graph()
    print(f"Loaded graph with {len(graph)} nodes.")
    ranks = pagerank(graph)
    top = sorted(ranks.items(), key=lambda x: x[1], reverse=True)[:20]
    print("Top 20 URLs by PageRank:")
    for url, score in top:
        print(f"{score:.6f} {url}")

def main():
    # Parse the command-line arguments
    args = parse_args()
    
    subreddits = args.subreddits.split(',')  # Split the comma-separated list of subreddits
    sizeMB = args.sizeMB
    outputDir = args.outputDir

    # Make sure the output directory exists
    generate_directory(outputDir)

    # For debugging, print the arguments to confirm everything is correct
    print(f"Subreddits to crawl: {subreddits}")
    print(f"Output directory: {outputDir}")
    print(f"Scraping up to {sizeMB} MB of data")

    #for dupe check
    load_submission_ids()
    load_crawled_links()
    check_prev_reddit_jsons()
    check_prev_crawled_links_jsons()
    print(len(crawled_links))
    print(len(seen_submission_ids))
    #generates directory if not already generated
    generate_directory(DIRECTORY_NAME)
    print(get_directory_size("./Reddit_Data_Chunks"))

    #crawls reddit
    try:
        for sub in subreddits:
            t = threading.Thread(target = crawl, args=(sub,400))
            t.daemon = True
            threads1.append(t)
            t.start()

        while any(t.is_alive() for t in threads1):
            for t in threads1:
                t.join(timeout=1)

    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        for t in threads2:
            pass
        sys.exit(0)

    #link crawler
    max_num = get_latest_json_num()
    try:
        for i in range(max_num):
            t = threading.Thread(target = crawl_links_in_queue, args=(i,))
            t.daemon = True
            threads2.append(t)
            t.start()

        while any(t.is_alive() for t in threads2):
            for t in threads2:
                t.join(timeout=1)            
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        for t in threads2:
            pass
        sys.exit(0)

    #connects links back to reddit posts
    update_chunks(FILENAME, "crawled_links", DIRECTORY_NAME)

    # Run PageRank analysis at the end
    run_pagerank()

if __name__ == "__main__":
    main()
