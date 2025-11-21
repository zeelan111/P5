import json
import os

directory = './posts'
posts = 0
users = 0

for user in os.scandir(directory):
    if not user.is_file():
        continue

    users += 1

    with open(user, "r") as json_file:
        json_list = list(json_file)
        
        for json_str in json_list:
            result = json.loads(json_str)

        posts += len(json_list)

print("Amount of posts: ", posts)
print("Amount of users: ", users)
