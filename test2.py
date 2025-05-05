import json
from pathlib import Path
import os
def count_total_instances(directory):
    total = 0
    folder = Path(directory)

    for i in range(43):  
        file_path = folder / f"crawled_links_{i}.json"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    total += len(data)
                else:
                    print(f"Warning: {file_path} does not contain a list.")
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Invalid JSON: {file_path}")

    return total

BATCH_SIZE = 10
MAX_FILE_SIZE = 10
DIRECTORY_NAME = './Reddit_Data_test'
FILENAME = 'data'
EXT = '.json'

#check size of json 
def get_file_size(file):
    return os.path.getsize(file) / (1024 * 1024)

def get_latest_json_num(file=FILENAME):
    i = 0
    while True:
        filename = f"{file}_{i}{EXT}"
        path = Path(DIRECTORY_NAME) / filename
        if not path.exists() or get_file_size(path) <= MAX_FILE_SIZE :
            return i
        i += 1

def dupe_eliminator(directory="./Reddit_Data_test", file=FILENAME):
    folder = Path(directory)
    max_num = get_latest_json_num(file)
    print(max_num)
    submission_id_set = set()
    for i in range(max_num):
        file_path = folder / f"data_{i}.json"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                datas = json.load(f)
            new_datas = []
            for data in datas:
                submission_id = data.get("ID")
                if submission_id not in submission_id_set:
                    new_datas.append(data)
                    submission_id_set.add(submission_id)
                else:
                    print("dupe found")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(new_datas,f,indent = 4)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Invalid JSON: {file_path}")

