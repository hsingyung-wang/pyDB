import urllib3
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
from io import StringIO
from headers import *
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json


url = "https://www.taifex.com.tw/cht/3/futContractsDate"
today = datetime.now().strftime("%Y/%m/%d")
print(f"爬取日期: {today}")


response = requests.get(url,headers=TAIFEX_OI)
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.table
df = pd.read_html(StringIO(str(table)))
df = df[0]
df.drop(columns = df.columns[0],axis=1,inplace=True)
df.columns=['商品名稱','身份別','多方交易口數','多方交易金額','空方交易口數','空方交易金額','交易淨口數','交易淨金額','多方未平倉口數','多方未平倉金額','空方未平倉口數','空方未平倉金額','未平倉淨口數','未平倉淨金額']
df_filter = df.loc[df['商品名稱'].isin(['臺股期貨','小型臺指期貨','微型臺指期貨'])]


# 從配置文件讀取連接信息
with open('config.json') as config_file:
    config = json.load(config_file)

mongo_user = config['mongo_user']
mongo_pass = config['mongo_pass']
mongo_host = config['mongo_host']

# 構建連接 URI
db_uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_host}/?retryWrites=true&w=majority&appName=FDB"

# 創建客戶端連接
client = MongoClient(db_uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# 進行數據操作...
client = MongoClient(db_uri, server_api=ServerApi('1'))
db = client['findata']
collection = db['txf_oi']

data_dict = df.to_dict('records')
result = collection.insert_many(data_dict)
print(f"成功插入 {len(result.inserted_ids)} 條文檔")

client.close()