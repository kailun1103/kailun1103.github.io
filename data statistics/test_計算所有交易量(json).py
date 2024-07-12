import os
import json

json_file_path = '2024_01_18-2024_01_24 - 資料集(json)'

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
                count += 1
            total += count

print(total)
