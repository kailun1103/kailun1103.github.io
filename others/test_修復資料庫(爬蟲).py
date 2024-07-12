import os
import json
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

driver = webdriver.Chrome()
driver.maximize_window()
driver.get(f'https://www.blockchain.com/explorer/transactions/btc')

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
                if item['Txn Input Details'] == '' or item['Txn Output Details'] == '':
                    hash = item['Txn Hash']
                    print('--------------------')
                    print(json_file)
                    print(time.ctime(time.time()))
                    print(f'hash: {hash}')
                    driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{hash}')
                    time.sleep(3)
                    # 假設抓不到交易的status，交易連查都查不到情況下

                    time.sleep(1)
                    # 獲得網頁垂直高度，假設抓不到json_button會調整頁面高度，假設抓到json按鈕元素才會往下跳
                    vertical_height = driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight );")
                    page_height = 0
                    while True:
                        page_height += 150
                        json_button = WebDriverWait(driver, 1).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[1]/button[2]'))
                        )   
                        if json_button:
                            json_button.click()
                            driver.execute_script(f"window.scrollTo(0, {page_height+100});")
                            break
                        else:
                            driver.execute_script(f"window.scrollTo(0, {page_height});")
                    time.sleep(1)

                    json_txn_detail = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre'))
                    )
                    data = json.loads(json_txn_detail.text)
                    input_details = []
                    output_details = []
                    for inp in data['inputs']:
                        input_details.append({
                            "inputHash": inp["address"] if inp["address"] else "Unknown",
                            "isContract": "false",
                            "amount": "{:.8f}".format(inp["value"] / 10**8)
                        })
                    input_details = str(input_details).replace("'", '"')
                    for out in data['outputs']:
                        output_details.append({
                            "outputHash": out['address'] if out["address"] else "Unknown",
                            "isContract": "false",  # 沒有提供相關資訊，假設為False
                            "amount": "{:.8f}".format(out["value"] / 10**8)
                        })
                    output_details = str(output_details).replace("'", '"')
                    item['Txn Input Details'] = input_details
                    item['Txn Output Details'] = output_details
                    count += 1
            total += count

        # 儲存修改後的資料
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(json_data, outfile, ensure_ascii=False, indent=4)

print(total)
