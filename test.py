import json
from collections import Counter

# Subreddits to track
target_subreddits = {'leagueoflegends', 'genshin_impact_leaks', 'honkaistarrail_leaks', 'wutheringwavesleaks'}

# Load data
file_path = 'Reddit_Data/data_1.json'
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Count matching subreddits
subreddit_counter = Counter()

for post in data:
    subreddit = post.get('subreddit', '').lower()
    if subreddit in target_subreddits:
        subreddit_counter[subreddit] += 1

# Print results
for sub in target_subreddits:
    print(f"{sub}: {subreddit_counter[sub.lower()]}")
