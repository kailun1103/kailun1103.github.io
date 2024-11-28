import os
import json


total = 0

with open('gcn_dataset for training/all dust', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    count = 0
    for item in json_data:
        count +=1
    total += count

print(total)