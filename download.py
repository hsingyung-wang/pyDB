import os
from urllib.request import urlretrieve
import zipfile
from datetime import datetime, timedelta
import pandas as pd

#儲存位置
save_dir = os.path.join(os.getcwd(), "dailydata","tx_zip")
today = datetime.today()
yesterday = today

#多次下載數據
for daycnt in range(1, 61):
    path = yesterday.strftime('%Y_%m_%d')
    name = 'Daily_' + path + '.zip'
    download_path = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/' + name
    
    
    # 检查并创建目录
    os.makedirs(save_dir, exist_ok=True)
    
    try:
        # 下载 ZIP 文件
        filename, _ = urlretrieve(download_path)
        print(f"下載完成：{download_path}")
        
        # 解壓縮直接讀取csv
        with zipfile.ZipFile(filename) as z:
            # 取得 ZIP 中的 CSV 文件名
            csv_name = next(f for f in z.namelist() if f.lower().endswith('.csv'))
            
            # 打開讀取data
            with z.open(csv_name) as f:
                df = pd.read_csv(f, encoding='big5', low_memory=False)
            
            print(f"CSV 文件讀取，共 {len(df)} 筆數據，開始整理數據")
        
        #篩選台指期貨
        df['商品代號'] = df['商品代號'].str.strip()
        df_tx = df[df['商品代號'] == 'TX'].copy()  # 使用.copy()避免SettingWithCopyWarning
        
        # 排除差價合約
        df_tx.loc[:, '到期月份(週別)'] = df_tx['到期月份(週別)'].astype(str)
        df_tx = df_tx[~df_tx['到期月份(週別)'].str.contains('/')]
        front_month = str(pd.to_numeric(df_tx['到期月份(週別)']).min())
        
        # 僅保留近月合约
        df_tx = df_tx[df_tx['到期月份(週別)'].str.strip() == front_month]
        
        # 確保價格是數值
        df_tx['成交價格'] = pd.to_numeric(df_tx['成交價格'], errors='coerce')
        
        # 處理時間
        df_tx['datetime'] = pd.to_datetime(
            df_tx['成交日期'].astype(str) + ' ' + 
            df_tx['成交時間'].astype(str).str.zfill(6),
            format='%Y%m%d %H%M%S'
        )
        
        # 設置時間索引
        df_tx.set_index('datetime', inplace=True)
        
        # 整理1分K棒
        ohlc = df_tx.resample('1Min').agg({
            '成交價格': [
                ('開盤價', 'first'),
                ('最高價', 'max'),
                ('最低價', 'min'),
                ('收盤價', 'last')
            ]
        })
        
        # 刪除沒數據的K
        ohlc = ohlc.dropna()
        ohlc.reset_index(inplace=True)
        
        # 重設欄位
        ohlc.columns = ['datetime', '開盤價', '最高價', '最低價', '收盤價']
        
        # 增加1分鐘，15:01為第一根K
        ohlc['datetime'] = ohlc['datetime'] + pd.Timedelta(minutes=1)  # 修正这里的拼写错误
        
        # 新增時間與日期
        ohlc['日期'] = ohlc['datetime'].dt.strftime('%Y-%m-%d')
        ohlc['時間'] = ohlc['datetime'].dt.strftime('%H:%M:%S')
        
        # 轉成Multichart使用的txt
        result = ohlc[['日期', '時間', '開盤價', '最高價', '最低價', '收盤價']]
        txt_content = result.apply(
            lambda row: f"{row['日期']},{row['時間']},{row['開盤價']},{row['最高價']},{row['最低價']},{row['收盤價']}",
            axis=1
        ).str.cat(sep='\n')
        
        #創目錄
        #txt_file = os.path.join(download_path, 'tx_1min')
        #os.makedirs(txt_file, exist_ok=True)
        
        # 保存txt文件
        tx_dir = os.path.join(save_dir, yesterday.strftime('%Y%m%d') + '.txt')
        
        with open(tx_dir, 'w', encoding='utf-8') as f:
            f.write(txt_content)
            
        print(f"已完成並存在：{tx_dir}")
        
    except Exception as e:
        print(f"處理過程中發生錯誤：{e}")
        
    finally:
        yesterday = yesterday - timedelta(days=1)
        # 完成後清理暫時存檔案
        if os.path.exists(filename):
            os.remove(filename)