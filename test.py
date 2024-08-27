import urllib3
import requests
from bs4 import BeautifulSoup

#處理資料套件
import pandas as pd
from datetime import date

def extract_taifex_oi_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 找到台指期貨的表格行
    rows = soup.find_all('tr', class_='12bk')
    target_rows = []
    for row in rows:
        if '臺股期貨' in row.text:
            target_rows.append(row)
    
    # 提取數據
    data = []
    categories = ['自營商', '投信', '外資']
    for row, category in zip(target_rows, categories):
        cells = row.find_all('td')
        net_oi = cells[-2].text.strip()
        net_oi = int(net_oi.replace(',', ''))
        data.append({
            '商品名稱': '臺股期貨',
            '身份別': category,
            '未平倉餘額多空淨額口數': net_oi
        })
    
    # 創建 DataFrame
    df = pd.DataFrame(data)
    return df

# 假設 HTML 內容存儲在變量 html_content 中
url = "https://www.taifex.com.tw/cht/3/futContractsDate"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.table

html_content = str(table)

result_df = extract_taifex_oi_data(html_content)

print("台指期貨未平倉餘額多空淨額：")
print(result_df)

# 如果你想將結果保存為 CSV 文件
result_df.to_csv('taifex_oi_data.csv', index=False, encoding='utf-8-sig')