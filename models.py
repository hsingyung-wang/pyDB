from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from datetime import datetime
import json


class FuturesDataModel:
    def __init__(self) -> None:
        with open('config.json') as config_file:
            config = json.load(config_file)
        self.mongo_user = config['mongo_user']
        self.mongo_pass = config['mongo_pass']
        self.mongo_host = config['mongo_host'] 

    def connect_mongo(self):
    
        #構建連接 URI
        db_uri = f"mongodb+srv://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}/?retryWrites=true&w=majority&appName=FDB"

        #創建客戶端連接
        client = MongoClient(db_uri, server_api=ServerApi('1'))

        try:
            client.admin.command('ping')
            msg = "Pinged your deployment. You successfully connected to MongoDB!"
            return client
        except Exception as e:
            return e

    def save_to_mongo(collection,date,data):    
        #檢查是否已存在該日期數據
        existing_data = collection.find_one({'date':date})
        if existing_data:
            print(f"{date}的數據已存在,跳過存入")
            return
    

        data_dict = data.to_dict('records')
        for record in data_dict:
            record['date'] = datetime.strptime(date,"%Y/%m/%d")

    
        result = collection.insert_many(data_dict)
        print(f"成功插入 {len(result.inserted_ids)} 條文檔")

    def print_mongo(self,db_name,collection_name):
    #可選：驗證插入的數據
    # 重新連接並查詢數據
        db = self.client[db_name]
        collection = db[collection_name]

        # 查詢並打印前幾條數據
        for doc in collection.find().limit(5):
            print(doc)


    def query_by_date(db,collection,start_date =None,end_date=None,limit=5):
        query={}
        if start_date and end_date:
            query['date'] = {
                '$gte': datetime.strptime(start_date, "%Y/%m/%d"),
                '$lte': datetime.strptime(end_date, "%Y/%m/%d")
            }
        elif start_date:
            query['date'] = {'$gte': datetime.strptime(start_date, "%Y/%m/%d")}
        elif end_date:
            query['date'] = {'$lte': datetime.strptime(end_date, "%Y/%m/%d")}

        cursor = collection.find(query).sort("date", ASCENDING).limit(limit)
        results = list(cursor)
        return results





