import json

def get_unique_addresses(input_file, output_file):
    # 讀取 JSON 文件
    with open(input_file, 'r') as f:
        data = json.load(f)

    # 使用集合來去除重複地址
    unique_addresses = list({item['address']: item for item in data}.values())

    # 將結果寫入新的 JSON 文件
    with open(output_file, 'w') as f:
        json.dump(unique_addresses, f, indent=4)

    print(f"獨立地址已保存到 {output_file}")
    return unique_addresses

# 使用示例
input_file = 'dust_addresses.json'
output_file = 'unique_dust_addresses.json'

unique_addresses = get_unique_addresses(input_file, output_file)

# 打印結果
print("獨立地址列表：")
for address in unique_addresses:
    print(address['address'])