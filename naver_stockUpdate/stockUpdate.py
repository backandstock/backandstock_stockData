import pymysql
import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET


conn = pymysql.connect(host='springboot-db2.cf0ijdzgtiko.ap-northeast-2.rds.amazonaws.com',
                       user='miniBacktesting',
                       password='back20220302',
                       db='miniBacktesting',
                       charset='utf8')
cur = conn.cursor()

sql = "select * from miniBacktesting.stock"
a = cur.execute(sql)
result = pd.read_sql_query(sql,conn)
stockInfo = result[[ 'market', 'public_date', 'stock_code', 'stock_name']].drop_duplicates("stock_code")
newDf = pd.DataFrame(columns = result.columns[1:])

count = 3
nullStockCodes = []
startDate = 20220201
endDate = 20220401

for idx, row in stockInfo.iloc[0:1].iterrows():
    if (row.stock_code == "001"):
        stockCode = "KOSPI"
    elif row.stock_code == "101":
        stockCode = "KOSDAQ"
    else:
        stockCode = row.stock_code

    print(stockCode)
    url = f"https://fchart.stock.naver.com/sise.nhn?symbol={stockCode}&timeframe=month&count={count}&requestType=0"
    r = urllib.request.urlopen(url)
    xml_data = r.read().decode('EUC-KR')
    root = ET.fromstring(xml_data)

    targetStockList = []
    for index, each in enumerate(root.findall('.//item')):
        temp = each.attrib["data"].split("|")
        if (int(temp[0]) >= startDate and int(temp[0]) < endDate):
            targetStockList.append(temp)

        if len(targetStockList) != 2:
            nullStockCodes.append(row.stock_code)

        close_date = int(temp[0])
        open = float(temp[1])
        high = float(temp[2])
        low = float(temp[3])
        close = float(temp[4])
        volume = int(temp[5])
        public_date = row.public_date
        stock_code = row.stock_code
        market = row.market
        stock_name = row.stock_name

        if (int(temp[0]) >= startDate and int(temp[0]) < endDate):
            if stockCode == "KOSPI" or stockCode == "KOSDAQ":

                newDf = newDf.append({'close': close * 100,
                                      'close_date': close_date,
                                      'high': high * 100,
                                      'low': low * 100,
                                      "market": market,
                                      'open': open * 100,
                                      'public_date': public_date,
                                      'stock_code': stock_code,
                                      'stock_name': stock_name,
                                      'transaction': 0,
                                      'volume': volume,
                                      'yield_pct': 0

                                      }, ignore_index=True)
            else:

                newDf = newDf.append({'close': close,
                                      'close_date': close_date,
                                      'high': high,
                                      'low': low,
                                      "market": market,
                                      'open': open,
                                      'public_date': public_date,
                                      'stock_code': stock_code,
                                      'stock_name': stock_name,
                                      'transaction': 0,
                                      'volume': volume,
                                      'yield_pct': 0

                                      }, ignore_index=True)

newDf.sort_values(["stock_code", "close_date"], inplace = True)
newDf["yield_pct"]= newDf.sort_values(["stock_code", "close_date"]).groupby(['stock_code']).close.pct_change()
newDf.dropna(inplace = True)
# Create a new record
sql = "INSERT INTO `stock` (`close`, `close_date`, `high`, `low`, `market`, `open`,`public_date`, `stock_code`, `stock_name`, `transaction`,`volume`,`yield_pct` ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"

for idx, row in newDf.iloc[0:2].iterrows():
# Execute the query
    cur.execute(sql, (row.close, row.close_date, row.high, row.low,
                      row.market,row.open, row.public_date, row.stock_code,
                     row.stock_name, row.transaction, row.volume, row.yield_pct))

# the connection is not autocommited by default. So we must commit to save our changes.
    conn.commit()