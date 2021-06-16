from finlab.crawler import (
    widget,

    crawl_price,
    crawl_monthly_report,
    crawl_finance_statement_by_date,
    update_table,

    table_exist,
    table_latest_date,
    table_earliest_date,

    date_range, month_range, season_range
)

import sqlite3
import os
import datetime
import tqdm
from io import StringIO
from dateutil.rrule import rrule, DAILY, MONTHLY

conn = sqlite3.connect(os.path.join('data', "data.db"))

Dictionary = {
    "A":"更新每日價位",
    "B":"更新月報",
    "C":"更新季報",
}

print("動作列表代號: \n ", Dictionary)
Execution = input("欲執行動作: " )

date = []
table = 'N/A'

if Execution == "A":
    table = 'price'
    function = crawl_price
elif Execution == "B":
    table = 'monthly_revenue'
    function = crawl_monthly_report
elif Execution == "C":
    table = 'finance_statement'
    function = crawl_finance_statement_by_date
else:
    print("重來一次")

if Execution == "A" or Execution == "B":
    earliest_date = table_earliest_date(conn, table)
    latest_date = table_latest_date(conn, table)
    print("資料庫", table, "儲存日期為: 從", earliest_date, " 到 ", latest_date )

Cfrom_Date = input("是否手動輸入起始更新日期(Y/N):")
if Cfrom_Date == "Y":
    from_year , from_month, from_day = input("欲更新日期:").split()
    from_Date = from_year+"-"+from_month+"-"+from_day
    from_Date = datetime.datetime.strptime(from_Date, '%Y-%m-%d')
elif Execution == "C":
    from_Date = datetime.datetime.now()
else:
    # from_Date = latest_date
    from_Date = latest_date + datetime.timedelta(days=1)
    # from_Date = datetime.date(2010,1,1)
print("起始日期為:", from_Date)

Cto_Date = input("是否手動輸入最終更新日期(Y/N):")
if Cto_Date == "Y":
    to_year, to_month, to_day = input("欲更新日期:").split()
    to_Date = to_year + "-" + to_month + "-" + to_day
    to_Date = datetime.datetime.strptime(to_Date, '%Y-%m-%d')
else:
    to_Date = datetime.datetime.now()
print("最終日期為:", to_Date)


if Execution == "A":
    date = date_range(from_Date, to_Date)
elif Execution == "B":
    date = month_range(from_Date, to_Date)
elif Execution == "C":
    date = season_range(from_Date, to_Date)

update_table(conn, table, function, date)

print("Program End")

'''
每季財報 時間區間補充：
關於每季財報爬取，選取的時間範圍要「包含」以下時段，就能夠爬到截止日期的財報囉！例如：

2010年3月31號 --> 2009 第四季
2010年5月15號 --> 2010 第一季
2010年8月14號 --> 2010 第二季
2010年11月14號 --> 2010 第三季
所以假如我們想要抓2018年的第一季，就可以輸入一個時間範圍，例如：0

2018年5月01號 到 2018年5月31號
因為這段時間有包含 2018年5月15號，所以就會下載 2018年第一季的財報喔！
'''

# dataA = datetime.datetime.strptime(dataA, '%Y-%m-%d')
# month_range(datetime.date(2018,1,1), datetime.date(2018,2,1))
# season_range(datetime.date(2018,1,1), datetime.date(2018,2,1))
# 從widget去找輸入財報更新14:40
# widget(conn, 'price', crawl_price, date_range)
# widget(conn, 'monthly_revenue', crawl_monthly_report, month_range)
# widget(conn, 'finance_statement', crawl_finance_statement_by_date, season_range)