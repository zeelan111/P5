import json
import os
#import time

#st = time.time()
directory = './posts'
postdict = {}
threadlessposts = []

keys = ["post_id", "user_id", "instance", 
        "date", "text", "langs", 
        "like_count", "reply_count", "repost_count", 
        "reply_to", "replied_author", "thread_root", 
        "thread_root_author", "repost_from", "reposted_author", 
        "quotes", "quoted_author", "labels", 
        "sent_label", "sent_score"]

# Reads all files in the directory
for user in os.scandir(directory):
    # If there is no file, skip the entry
    if not user.is_file():
        continue

    with open(user, "r") as json_file:
        # Make list of each post
        json_list = list(json_file)

        for json_str in json_list:
            # Make each JSON object a string
            result = json.loads(json_str)

            # Check if it is a reply to another post
            if result["reply_to"]:  
                # Make key in dict of thread roots
                postdict.setdefault(result["thread_root"], [])

                # Add interaction to root
                postdict[result["thread_root"]].append(result["post_id"])
                """print(result["post_id"], "is a reply to", result["reply_to"], "with", result["thread_root"], "as root")"""
            else:
                threadlessposts.append(result["post_id"])

threadposts = 0

for x in postdict:
    threadposts += len(postdict[x])
    if len(postdict[x]) > 5:
        print(len(postdict[x]), x , postdict[x])

#print(len(postdict))
print("Posts without interactions:", len(threadlessposts))
print("Posts with interactions:", threadposts)

#DT = time.time() - st
#print("Delta:", DT, ", Est time:", (2363248/1000)*DT)

"""
# Open specific file         
with open("./posts/2780.jsonl", "r") as json_file:
    # Make list of each post
    json_list = list(json_file)

    for json_str in json_list:
        # Make each JSON object a string
        result = json.loads(json_str)

        # Check if it is a reply to another post
        if result["reply_to"]:  
            postdict.setdefault(result["thread_root"], [])
            postdict[result["thread_root"]].append(result["post_id"])
            #print(result["post_id"], "is a reply to", result["reply_to"], "with", result["thread_root"], "as root")
        
#print(postdict, "\n", len(postdict))
for x in postdict:
    print(len(postdict[x]), postdict[x])
#print(postdict[69137155])
"""