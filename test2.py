import json
from pathlib import Path
import re
import os

MAX_FILE_SIZE = 9.5

def load_json_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def save_json_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def get_file_size(file):
    return os.path.getsize(file) / (1024 * 1024)

def get_latest_json_num(file, directory):
    i = 0
    while True:
        filename = f"{file}_{i}.json"
        path = Path(directory) / filename
        if not path.exists() or get_file_size(path) < MAX_FILE_SIZE:
            return i
        try:
            with open(path, 'r', encoding='utf-8') as f:
                json.load(f)
        except json.JSONDecodeError:
            print(f"[Corrupt JSON] {filename} is invalid — skipping.")
            return i  
        i += 1

def get_all_crawled_info(file, directory):
    max_num = get_latest_json_num(file, directory)
    all_info = []
    print(max_num)
    for i in range(max_num + 1):
        filepath = Path(directory) / f"{file}_{i}.json"
        all_info.extend(load_json_file(filepath))
    return all_info

def update_chunks(chunk, file, directory):
    max_num = get_latest_json_num(chunk, directory)
    print(max_num)
    crawled_info = get_all_crawled_info(file, directory)
    for i in range(max_num + 1):
        filepath = Path(directory) / f"{chunk}_{i}.json"
        chunk_info = load_json_file(filepath)
        for object in chunk_info:
            sid = object.get("ID")
            print(sid)
            matches = [entry for entry in crawled_info if entry.get("from_reddit") == sid]

            url_titles = [m.get("title") for m in matches if m.get("from") == "url" and m.get("title")]
            body_links = [(m.get("URL"), m.get("title")) for m in matches if m.get("from") == "body" and m.get("URL") and m.get("title")]
            comments_links = [(m.get("URL"), m.get("title")) for m in matches if m.get("from") == "comment" and m.get("URL") and m.get("title")]

            object["url_title"] = url_titles
            print(url_titles)
            object["body_links"] = body_links
            object["comments_links"] = comments_links
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(chunk_info, f, indent = 4)

update_chunks("chunk", "crawled_links", "Reddit_Data_Chunks - Copy")
    