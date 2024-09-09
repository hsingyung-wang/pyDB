#下載資料套件
import urllib3
import requests
from bs4 import BeautifulSoup

#處理資料套件
import pandas as pd
from datetime import date

#畫圖套件



def scrape_taifex_oi_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # 找到包含未平倉口數的表格
    #tables = soup.find_all('table', {'class': 'table_f'})
    table = soup.table
    print(table)
    df =  pd.read_html(str(table))
    df = df.iloc[2]
    #df.columns=['商品名稱','身份別','多方口數','多方契約金額','空方口數','空方契約金額','多方淨額口數','多方淨額契約金額','']

    print(df)

    target_table = None
    for table in table:
        if '多空淨額' in table.text and '外資' in table.text:
            target_table = table
            break
    
    if not target_table:
        print("無法找到目標表格，請檢查網頁結構是否有變化。")
        return None
    
    # 提取數據
    rows = []
    for tr in target_table.find_all('tr')[2:]:  # 跳過表頭行
        cols = tr.find_all('td')
        if len(cols) >= 9:  # 確保行有足夠的列
            contract = cols[0].text.strip()
            if '臺股期貨' in contract:
                foreign_long = int(cols[3].text.strip().replace(',', ''))
                foreign_short = int(cols[4].text.strip().replace(',', ''))
                investment_trust_long = int(cols[5].text.strip().replace(',', ''))
                investment_trust_short = int(cols[6].text.strip().replace(',', ''))
                dealer_long = int(cols[7].text.strip().replace(',', ''))
                dealer_short = int(cols[8].text.strip().replace(',', ''))
                
                rows.append({
                    '契約': contract,
                    '外資多方未平倉': foreign_long,
                    '外資空方未平倉': foreign_short,
                    '投信多方未平倉': investment_trust_long,
                    '投信空方未平倉': investment_trust_short,
                    '自營商多方未平倉': dealer_long,
                    '自營商空方未平倉': dealer_short
                })
    
    df = pd.DataFrame(rows)
    return df

def main():
    url = "https://www.taifex.com.tw/cht/3/futContractsDate"
    df = scrape_taifex_oi_data(url)
    if df is not None:
        print("抓取到的台指期貨未平倉口數數據：")
        print(df)
        
        # 將數據保存為CSV文件
        #df.to_csv('taifex_oi_data.csv', index=False, encoding='utf-8-sig')
        print("數據已保存到 taifex_oi_data.csv 文件中")
    else:
        print("無法獲取數據。")

if __name__ == "__main__":
    main()