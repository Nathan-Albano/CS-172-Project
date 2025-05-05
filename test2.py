import json
from pathlib import Path

def count_total_instances(directory):
    total = 0
    folder = Path(directory)

    for i in range(43):  # 0 to 42 inclusive
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

# Example usage:
directory_path = "./Reddit_Data"  # change this to your actual folder path
total_instances = count_total_instances(directory_path)
print(f"Total instances across all JSONs: {total_instances}")