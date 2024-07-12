import json

# 读取原始JSON文件
with open('2024_01_18-2024_01_24 - hash.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 计算每个文件中应包含的条目数
num_files = 80
entries_per_file = len(data) // num_files

# 分割数据并写入新的JSON文件
for i in range(num_files):
    start_idx = i * entries_per_file
    end_idx = (i + 1) * entries_per_file if i < num_files - 1 else len(data)
    chunk = data[start_idx:end_idx]

    with open(f'unique_hashes_part_{i + 1}.json', 'w', encoding='utf-8') as f:
        json.dump(chunk, f, ensure_ascii=False, indent=4)

print("分割完成！")
