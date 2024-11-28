import os
import json


with open('BTX_Transaction_data_2024_01_18_12-13.json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    count = 0
    for item in json_data:
        print(item['Txn Hash'])
        input_details = json.loads(item['Txn Input Details'])
        output_details = json.loads(item['Txn Output Details'])
        with open('gcn result/cluster -1/all dust(input output)-d-io/161個(粉塵接收者and粉塵金額散佈者).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all dust(input output)-d-io/896個(粉塵攻擊者 接收粉塵交易後再攻擊).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all dust(output)-d-o/342個(粉塵接收者).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all dust(output)-d-o/534個(粉塵混雜正常交易接收者).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all txn(input output)-t-io/1365個(粉塵接收者and正常金額散佈者).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all txn(input output)-t-io/7774個(粉塵攻擊者 接收 正常交易金額後再攻擊).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all txn(output)-t-o/40個(粉塵混雜正常交易接收者).json', 'r', encoding='utf-8') as infile:
        # with open('gcn result/cluster -1/all txn(output)-t-o/55個(粉塵接收者).json', 'r', encoding='utf-8') as infile:
            json_data2 = json.load(infile)
            for item2 in json_data2:
                for detail in input_details:
                    if detail['inputHash'] == item2['address']:
                        detail['cluster'] = "-1(d-io)(dust receiver AND sent amount to dust attacker)"
                        # detail['cluster'] = "-1(d-io)(receive dust amount And dust attacker)"

                        # detail['cluster'] = "-1(d-o)(dust receiver)"
                        # detail['cluster'] = "-1(d-o)(dust and nor receiver)"

                        # detail['cluster'] = "-1(t-io)(dust receiver AND sent amount to nor address)"
                        # detail['cluster'] = "-1(t-io)(receive nor amount And dust attacker)"

                        # detail['cluster'] = "-1(t-o)(dust receiver)"
                        # detail['cluster'] = "-1(t-o)(dust and nor receiver)"

                for detail in output_details:
                    if detail['outputHash'] == item2['address']:
                        detail['cluster'] = "-1(d-io)(dust receiver AND sent amount to dust attacker)"
                        # detail['cluster'] = "-1(d-io)(receive dust amount And dust attacker)"

                        # detail['cluster'] = "-1(d-o)(dust receiver)"
                        # detail['cluster'] = "-1(d-o)(dust and nor receiver)"

                        # detail['cluster'] = "-1(t-io)(dust receiver AND sent amount to nor address)"
                        # detail['cluster'] = "-1(t-io)(receive nor amount And dust attacker)"

                        # detail['cluster'] = "-1(t-o)(dust receiver)"
                        # detail['cluster'] = "-1(t-o)(dust and nor receiver)"

                item['Txn Input Details'] = json.dumps(input_details)
                item['Txn Output Details'] = json.dumps(output_details)

with open('updated_BTX_Transaction_data.json', 'w', encoding='utf-8') as file:
    json.dump(json_data, file, indent=2, ensure_ascii=False)
