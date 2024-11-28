import os
import json

json_file_path = 'test'

total = 0
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            count = 0
            for item in json_data:
                if item['Dust Bool'] == '1':
                    count +=1
            total += count

print(total)
