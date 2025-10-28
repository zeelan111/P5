import requests
import json
from stuff import *


def check_for_factual_claim(s):
    model = requests.post(
    "http://localhost:11434/api/generate",
    json={
        "model": "llama3", 
        "prompt": ahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh + " " + s
    }
)
    full_output = ""
    for line in model.text.splitlines():
        try:
            chunk = json.loads(line)
            if "response" in chunk:
                full_output += chunk["response"]
            if chunk.get("done", False):
                break
        except json.JSONDecodeError:
            continue
    return full_output

sentence = [
    #"cats are cute",
    #"Albert Einstein is a genius",
    #"Albert Einstein was a genius because he invented chess",
    "Jazz is the most relaxing genre of music",
    "Mount Everest is the tallest mountain on Earth",
    "The human body has four hearts",
    "Chocolate ice cream is better than vanilla",
    "The Pacific Ocean is the largest ocean on Earth",
    "Sharks can breathe air like humans"
]


for i in sentence:
    print(f'{i}: {check_for_factual_claim(i)}')