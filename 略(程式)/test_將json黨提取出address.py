import os
import json

json_file_path = 'test'
output_file_path = 'output_unique_hashes_filtered.json'

total = 0
unique_hashes = set()

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            count = 0
            for item in json_data:
                input_details = json.loads(item['Txn Input Details'])
                output_details = json.loads(item['Txn Output Details'])
                for detail in input_details:
                    count += 1
                    unique_hashes.add(detail['inputHash'])
                    print(detail['inputHash'])
                for detail in output_details:
                    count += 1
                    unique_hashes.add(detail['outputHash'])
                    print(detail['outputHash'])
            total += count

print(f"Total count: {total}")
print(f"Unique hash count before filtering: {len(unique_hashes)}")

# 过滤掉包含 'Unknown_' 的哈希值
filtered_hashes = [hash_value for hash_value in unique_hashes if 'Unknown_' not in hash_value]

# 将过滤后的唯一哈希值转换为列表并保存到新的 JSON 文件
unique_hashes_list = [{"address": hash_value} for hash_value in filtered_hashes]

print(f"Unique hash count after filtering: {len(unique_hashes_list)}")

with open(output_file_path, 'w', encoding='utf-8') as outfile:
    json.dump(unique_hashes_list, outfile, ensure_ascii=False, indent=2)

print(f"Filtered unique hashes have been saved to {output_file_path}")