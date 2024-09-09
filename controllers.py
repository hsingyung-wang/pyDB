import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from io import StringIO
from models import FuturesDataModel
from headers import *
from schema import *
import json

class FuturesDataController:
    def __init__(self):
        self.model = FuturesDataModel()
    
    def crawler_taifex_data(self,date):
        url = "https://www.taifex.com.tw/cht/3/futContractsDate"
        
        print(f"爬取日期: {date}")
        params = {
               'queryType': '2',
               'goDay': '',
               'doQuery': '1',
               'dateaddcnt': '',
               'queryDate': date
           }
        
        try:
            response = requests.post(url,headers=TAIFEX_OI,data=params)
            response.raise_for_status() #檢查請求是否成功

            if "日期" not in response.text or "商品" not in response.text:
                print("響應中沒有找到預期的數據")
                return None
            
            print("爬取成功")
            soup = BeautifulSoup(response.content,'html.parser')
            table = soup.table
            df = pd.read_html(StringIO(str(table)))
            df = df[0]
            df.drop(columns = df.columns[0],axis=1,inplace=True)
            df.columns=['商品名稱','身份別','多方交易口數','多方交易金額','空方交易口數','空方交易金額','交易淨口數','交易淨金額','多方未平倉口數','多方未平倉金額','空方未平倉口數','空方未平倉金額','未平倉淨口數','未平倉淨金額']

            #只抓取台指期貨契約
            df_filter = df.loc[df['商品名稱'].isin(['臺股期貨','小型臺指期貨','微型臺指期貨'])]


            # 定義需要進行數值轉換的列
            numeric_columns = ['多方交易口數', '多方交易金額', '空方交易口數', '空方交易金額', '交易淨口數', 
                               '交易淨金額', '多方未平倉口數', '多方未平倉金額', '空方未平倉口數', '空方未平倉金額', 
                               '未平倉淨口數', '未平倉淨金額']
            # 對每一列進行處理-轉化成數字
            
            for col in numeric_columns:
                df_filter[col] = pd.to_numeric(df_filter[col].astype(str).str.replace(',', ''), errors='coerce')
            print("轉換成功")
            return df_filter
        except requests.RequestException as e:
            print(f"請求錯誤: {e}")
        except Exception as e:
            print(f"抓取數據時發生錯誤：{e}")
            return None
           
    def crawler_twse_data(self,date:str):
        #url = "https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html"
        #url = "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"
        url = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
        url2 = "https://www.twse.com.tw/rwd/zh/TAIEX/MFI94U"
        params = {
            'response':'json',
            'date':f'{date}'
        }
        print(f"爬取日期: {date}")

        try:  
            response = requests.get(url,data=params,headers=TWSE_DOWN)
            response.raise_for_status() #檢查請求是否成功
            response2 =requests.get(url2,data=params,headers=TWSE_DOWN)
            response2.raise_for_status()
            

            data = json.loads(response.text)
            data2 = json.loads(response2.text)
            df = pd.DataFrame(data['data'],columns=['date','open','high','low','close'])
            df2 = pd.DataFrame(data2['data'],columns=['date','tri_close'])
            #去除符號轉float
            df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda x: x.str.replace(',', '').astype(float), axis=1)
            df2.iloc[:,1:] = df2.iloc[:,1:].apply(lambda x: x.str.replace(',','').astype(float), axis=1)
            merged_df = pd.merge(df,df2,on='date',how='outer')


            #merged_df日期格式轉換
            merged_df[['year', 'month', 'day']] = merged_df['date'].str.split('/', expand=True)
            merged_df['year'] = merged_df['year'].astype(int) + 1911
            merged_df['date'] = merged_df['year'].astype(str) + '-' + merged_df['month'].str.zfill(2) + '-' + merged_df['day'].str.zfill(2)
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            merged_df = merged_df.drop(columns=['year','month','day'])

            return merged_df

        except Exception as e:
            print(f"抓取數據時發生錯誤：{e}")
            return None

    """
    date = 20240101
    """

    def update_twse_data(self,date):
        df_data = self.crawler_twse_data(date)
        if df_data is None or df_data.empty:
            return "無法獲取證交所數據或數據為空"
        
        #轉成MongoDB格式
        data = {
            "querydate":df_data['date'],
            "taiex":[]
        }

        for _,row in df_data.iterrows():
            contract = {
                "date":row["date"],
                "open":row["open"],
                "high":row["high"],
                "low":row["low"],
                "close":row["close"],
                "tri_close":row["tri_close"]
            }

        data["taiex"].append(contract)

        if not data["taiex"]:
            return "沒有有效的加權指數數據可以插入"
        try:
            self.model.connect_mongo()
            success = self.model.save_to_mongo("findata","taiex",data)
            return f"成功{'保存' if success else '更新'} {len(data['taiex'])} 條記錄到 MongoDB"
        except Exception as e:
            return f"保存到 MongoDB 時發生錯誤：{e}"
        finally:
            self.model.close_mongo()
     


    def update_taifex_data(self,date):
        df_data = self.crawler_taifex_data(date)
        if df_data is None or df_data.empty:
            return "無法獲取數據或數據為空"
        
        #轉成MongoDB格式
        data = {
            "日期":datetime.strptime(date,"%Y/%m/%d"),
            "期貨契約":[]
        }

        
        
        for _, row in df_data.iterrows():
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
        
        if not data["期貨契約"]:
            return "沒有有效的期貨契約數據可以插入"

        try:
            self.model.connect_mongo()
            success = self.model.save_to_mongo("findata","txf_oi",data)
            return f"成功{'保存' if success else '更新'} {len(data['期貨契約'])} 條記錄到 MongoDB"
        except Exception as e:
            return f"保存到 MongoDB 時發生錯誤：{e}"
        finally:
            self.model.close_mongo()
     