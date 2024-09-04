from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from datetime import datetime
import json
from typing import Optional, List, Dict, Any
import pandas as pd



class FuturesDataModel:
    def __init__(self) -> None:
        with open('config.json') as config_file:
            config = json.load(config_file)
        self.mongo_uri = f"mongodb+srv://{config['mongo_user']}:{config['mongo_pass']}@{config['mongo_host']}/?retryWrites=true&w=majority&appName=FDB"
        self.client = None

    def connect_mongo(self):
        try:
            self.client = MongoClient(self.mongo_uri, server_api=ServerApi('1'))
            self.client.admin.command('ping')
            print("成功連接到 MongoDB!")
            return self.client
        except Exception as e:
            print(f"連接 MongoDB 失敗: {e}")
            raise

    def transofrm_to_mongo_format(sefl,df,date):
        data = {
            "日期":datetime.strptime(date,"%Y/%m/%d"),
            "期貨契約":[]
        }
        for _, row in df.iterrows():
            contract = {
                "商品名稱": row['商品名稱'],
                "身份別": row['身份別'],
                "多方交易口數": int(row['多方交易口數']),
                "多方交易金額": int(row['多方交易金額']),
                "空方交易口數": int(row['空方交易口數']),
                "空方交易金額": int(row['空方交易金額']),
                "交易淨口數": int(row['交易淨口數']),
                "交易淨金額": int(row['交易淨金額']),
                "多方未平倉口數": int(row['多方未平倉口數']),
                "多方未平倉金額": int(row['多方未平倉金額']),
                "空方未平倉口數": int(row['空方未平倉口數']),
                "空方未平倉金額": int(row['空方未平倉金額']),
                "未平倉淨口數": int(row['未平倉淨口數']),
                "未平倉淨金額": int(row['未平倉淨金額'])
            }
            data["期貨契約"].append(contract)

            return data

    def save_to_mongo_by_dataframe(self,db_name:str,collection_name:str,data:pd.DataFrame,date):
        if not self.client:
            self.connect_mongo()
        
        db = self.client[db_name]
        collection = db[collection_name]
        
        #檢查是否已存在該日期數據
        date = data['日期']
        existing_data = collection.find_one({'日期':date})
        if existing_data:
            print(f"{date}的數據已存在,跳過存入")
            return
    
        
        data_dict = data.to_dict('records')
        for record in data_dict:
            record['日期'] = datetime.strptime(date,"%Y/%m/%d")
        
        try:
            result = collection.insert_many(data)
            print(f"成功插入 {len(result.inserted_ids)} 條文檔")
        except Exception as e:
            print(f"插入數據時發生錯誤: {e}")

    def save_to_mongo(self,db_name:str,collection_name:str,data):
        if not data["期貨契約"]:
            raise ValueError("期貨契約列表為空")
        
        if not self.client:
            self.connect_mongo()

        db = self.client[db_name]
        collection = db[collection_name]
        
        existing_data = collection.find_one({'日期': data['日期']})

        

        try:
            result = collection.insert_one(data)
            print(f"成功插入 {len(result.inserted_ids)} 條文檔")

        except Exception as e:
            print(f"插入數據時發生錯誤: {e}")



    def close_mongo(self):
        if self.client:
            self.client.close()
            print("MongoDB 連接已關閉")


    def print_mongo(self,db_name,collection_name):
    #可選：驗證插入的數據
    # 重新連接並查詢數據
        db = self.client[db_name]
        collection = db[collection_name]

        # 查詢並打印前幾條數據
        for doc in collection.find().limit(5):
            print(doc)


    def query_by_date(self,db_name:str,collection_name:str,start_date =None,end_date=None,limit=5):
        if not self.client:
            self.connect_mongo()

        db = self.client[db_name]
        collection = db[collection_name]

        query={}
        if start_date and end_date:
            query['日期'] = {
                '$gte': datetime.strptime(start_date, "%Y/%m/%d"),
                '$lte': datetime.strptime(end_date, "%Y/%m/%d")
            }
        elif start_date:
            query['日期'] = {'$gte': datetime.strptime(start_date, "%Y/%m/%d")}
        elif end_date:
            query['日期'] = {'$lte': datetime.strptime(end_date, "%Y/%m/%d")}

        cursor = collection.find(query).sort("日期", ASCENDING).limit(limit)
        results = list(cursor)
        return results





