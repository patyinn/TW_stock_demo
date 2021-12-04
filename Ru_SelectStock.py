from finlab.data import Data
import pandas as pd
from matplotlib import pyplot as plt
# import datetime
import numpy as np
# import warnings
import math
import os
import openpyxl
import operator

def toSeasonal(df):
    season4 = df[df.index.month == 3]
    season1 = df[df.index.month == 5]
    season2 = df[df.index.month == 8]
    season3 = df[df.index.month == 11]

    season1.index = season1.index.year
    season2.index = season2.index.year
    season3.index = season3.index.year
    season4.index = season4.index.year - 1

    # 第一季即自己的資料
    newseason1 = season1
    # 第二季為 Q2 扣掉累積至Q1的資料
    # reindex_like: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.reindex_like.html
    # reindex_like: 將season1的資料依照season2的index重新排列
    newseason2 = season2 - season1.reindex_like(season2)
    # 第三季為 Q3 扣掉累積至Q2的資料
    newseason3 = season3 - season2.reindex_like(season3)
    newseason4 = season4 - season3.reindex_like(season4)

    newseason1.index = pd.to_datetime(newseason1.index.astype(str) + '-05-15')
    newseason2.index = pd.to_datetime(newseason2.index.astype(str) + '-08-14')
    newseason3.index = pd.to_datetime(newseason3.index.astype(str) + '-11-14')
    newseason4.index = pd.to_datetime((newseason4.index + 1).astype(str) + '-03-31')

    return newseason1.append(newseason2).append(newseason3).append(newseason4).sort_index()

# 把列出資料夾的程式碼寫成一個函式
def show_folder_content(folder_path, prefix=None, postfix=None):
    # print(folder_path + '，的資料夾內容：')

    files_list = []
    folder_content = os.listdir(folder_path)
    for item in folder_content:

        fullpath = os.path.join(folder_path, item)

        if os.path.isdir(fullpath):
            # print('資料夾：' + item)
            # 呼叫自己處理這個子資料夾
            files_list += show_folder_content(fullpath, prefix=prefix, postfix=postfix)

        elif os.path.isfile(fullpath):
            # print('檔案：' + item)
            if prefix:
                if item.startswith(prefix):
                    files_list.append(os.path.join(folder_path, item))
            elif postfix:
                if item.endswith(postfix):
                    files_list.append(os.path.join(folder_path, item))
            else:
                files_list.append(os.path.join(folder_path, item))
        # else:
            # print('無法辨識：' + item)
    return files_list

def mystrategy(data, date):

    股本 = data.get3(name='股本合計', n=1, start=date)
    price = data.get3(name='收盤價', n=120, start=date)
    當天股價 = price[:股本.index[-1]].iloc[-1]
    當天股本 = 股本.iloc[-1]
    市值 = 當天股本 * 當天股價 / 10 * 1000

    df1 = toSeasonal(data.get3(name='投資活動之淨現金流入（流出）', n=15, start=date))
    df2 = toSeasonal(data.get3(name='營業活動之淨現金流入（流出）', n=15, start=date))
    三年自由現金流 = (df1 + df2).iloc[-12:].mean()

    稅後淨利 = data.get3(name='本期淨利（淨損）', n=9, start=date)
    # 股東權益，有兩個名稱，有些公司叫做權益總計，有些叫做權益總額
    # 所以得把它們抓出來
    權益總計 = data.get3(name='權益總計', n=1, start=date)
    權益總額 = data.get3(name='權益總額', n=1, start=date)

    # 並且把它們合併起來
    權益總計.fillna(權益總額, inplace=True)

    股東權益報酬率 = ((稅後淨利.iloc[-4:].sum()) / 權益總計.iloc[-1]) * 100

    營業利益 = data.get3(name='營業利益（損失）', n=9, start=date)
    Revenue_Season = data.get3(name='營業收入合計', n=9, start=date)
    營業利益率 = 營業利益 / Revenue_Season
    前季營業利益率 = 營業利益.shift(1) / Revenue_Season.shift(1)
    營業利益年成長率 = (營業利益率.iloc[-1] / 營業利益率.iloc[-5] - 1) * 100
    八季營益率成長率 = (營業利益率 / 前季營業利益率 - 1) * 100

    當月營收 = data.get3(name='當月營收', n=12, start=date) * 1000
    年營收 = 當月營收.iloc[-12:].sum()
    市值營收比 = 市值 / 年營收

    MR_YearGrowth = data.get3(name='去年同月增減(%)', n=12, start=date)
    短期營收年增 = MR_YearGrowth.rolling(3).mean().reindex(index=MR_YearGrowth.index).iloc[-1]
    長期營收年增 = MR_YearGrowth.rolling(12).mean().reindex(index=MR_YearGrowth.index).iloc[-1]

    稅後淨利率 = 稅後淨利 / Revenue_Season
    去年稅後淨利率 = 稅後淨利率.shift(4)
    稅後淨利年增 = (稅後淨利率 - 去年稅後淨利率) / 去年稅後淨利率 * 100
    稅後淨利年增 = 稅後淨利年增
    短期淨利年增 = 稅後淨利年增.iloc[-1]
    長期淨利年增 = 稅後淨利年增[-4:].mean()

    INV = data.get3(name="存貨", n=3, start=date)
    OC = data.get3(name="營業成本合計", n=2, start=date)
    存貨周轉率 = OC.iloc[-1] / ((INV.iloc[-1] + INV.iloc[-2]) / 2) * 4
    前季存貨周轉率 = OC.iloc[-2] / ((INV.iloc[-2] + INV.iloc[-3]) / 2) * 4
    存貨周轉變化率 = (存貨周轉率 - 前季存貨周轉率) / 前季存貨周轉率 * 100

    rsv = (price.iloc[-1] - price.iloc[-60:].min()) / (price.iloc[-60:].max() - price.iloc[-60:].min())

    print(八季營益率成長率[八季營益率成長率 <= -30].dropna(axis=1, how="all").dropna(how="all"))
    condition_list = [
        市值 > 5e9,
        三年自由現金流 > 0,
        股東權益報酬率 > 15,
        營業利益年成長率 >= 0,
        八季營益率成長率[八季營益率成長率 <= -30].notnull().sum() < 2,
        # 市值營收比 < 5,
        短期營收年增 > 長期營收年增,
        短期淨利年增 > 長期淨利年增,
        短期營收年增 > 20,
        # 存貨周轉變化率 > 0,
        # rsv > 0.8
    ]

    select_stock = condition_list[0]
    for con in range(len(condition_list)):
        select_stock = select_stock & condition_list[con]
    print(select_stock)
    print(select_stock[select_stock])
    return select_stock[select_stock]

def backtest1(start_date, end_date, hold_days, data, weight='average', benchmark=None, stop_loss=None,
             stop_profit=None):
    # portfolio check
    if weight != 'average' and weight != 'price':
        print('Backtest stop, weight should be "average" or "price", find', weight, 'instead')

    # get price data in order backtest
    data.date = end_date
    price = data.get('收盤價', (end_date - start_date).days)
    # start from 1 TWD at start_date,
    end = 1
    date = start_date

    # record some history
    equality = pd.Series()
    nstock = {}
    transactions = pd.DataFrame()
    maxreturn = -10000
    minreturn = 10000

    def trading_day(date):
        if date not in price.index:
            temp = price.loc[date:]
            if temp.empty:
                return price.index[-1]
            else:
                return temp.index[0]
        else:
            return date

    def date_iter_periodicity(start_date, end_date, hold_days):
        date = start_date
        while date < end_date:
            yield (date), (date + datetime.timedelta(hold_days))
            date += datetime.timedelta(hold_days)

    def date_iter_specify_dates(start_date, end_date, hold_days):
        dlist = [start_date] + hold_days + [end_date]
        if dlist[0] == dlist[1]:
            dlist = dlist[1:]
        if dlist[-1] == dlist[-2]:
            dlist = dlist[:-1]
        for sdate, edate in zip(dlist, dlist[1:]):
            yield (sdate), (edate)

    if isinstance(hold_days, int):
        dates = date_iter_periodicity(start_date, end_date, hold_days)
    elif isinstance(hold_days, list):
        dates = date_iter_specify_dates(start_date, end_date, hold_days)
    else:
        print('the type of hold_dates should be list or int.')
        return None

    figure, ax = plt.subplots(2, 1, sharex=True, sharey=False)

    keep_list = []
    keep_idx = pd.Index(keep_list)
    for sdate, edate in dates:

        stock_list = []
        # select stocks at date
        data.date = sdate
        # https://stackoverflow.com/questions/39137506/map-to-list-error-series-object-not-callable
        stocks = mystrategy(data, sdate)
        # Idx = stocks.index
        # Idx = stocks.index.append([keep_idx])
        Idx = stocks.index.append([keep_idx]).drop_duplicates()
        print("回測的股票為: ", Idx)
        # hold the stocks for hold_days day
        s = price[Idx & price.columns][sdate:edate].iloc[1:]

        if s.empty:
            s = pd.Series(1, index=pd.date_range(sdate + datetime.timedelta(days=1), edate))
        else:
            if stop_loss != None:
                below_stop = ((s / s.bfill().iloc[0]) - 1) * 100 < -np.abs(stop_loss)
                below_stop = (below_stop.cumsum() > 0).shift(2).fillna(False)
                s[below_stop] = np.nan
            if stop_profit != None:
                above_stop = ((s / s.bfill().iloc[0]) - 1) * 100 > np.abs(stop_profit)
                above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                s[above_stop] = np.nan

            s.dropna(axis=1, how='all', inplace=True)
            keep_list = s.dropna(axis=1)
            keep_idx = pd.Index(keep_list.columns)

            # record transactions
            bprice = s.bfill().iloc[0]
            sprice = s.apply(lambda s: s.dropna().iloc[-1])
            transactions = transactions.append(pd.DataFrame({
                'buy_price': bprice,
                'sell_price': sprice,
                'lowest_price': s.min(),
                'highest_price': s.max(),
                'buy_date': pd.Series(s.index[0], index=s.columns),
                'sell_date': s.apply(lambda s: s.dropna().index[-1]),
                'profit(%)': (sprice / bprice - 1) * 100
            })).sort_index(ascending=True)

            s.ffill(inplace=True)
            s = s.sum(axis=1)
            # calculate equality
            # normalize and average the price of each stocks
            if weight == 'average':
                s = s / s.bfill().iloc[0]
            else:
                s = s / s.bfill()[0]
        # print some log
        print(sdate, '-', edate, '報酬率: %.2f' % (s.iloc[-1] / s.iloc[0] * 100 - 100), '%', 'nstock', len(Idx))
        benchmark1 = price['0050'][sdate:edate].iloc[1:]
        print(sdate, '-', edate, '的0050報酬率: %.2f' % (benchmark1.iloc[-1] / benchmark1.iloc[0] * 100 - 100), '%')
        maxreturn = max(maxreturn, s.iloc[-1] / s.iloc[0] * 100 - 100)
        minreturn = min(minreturn, s.iloc[-1] / s.iloc[0] * 100 - 100)

        # plot backtest result
        ((s * end - 1) * 100).plot(ax=ax[0])
        equality = equality.append(s * end)
        end = (s / s[0] * end).iloc[-1]

        if math.isnan(end):
            end = 1

        # add nstock history
        nstock[sdate] = len(stocks)

    print('每次換手最大報酬 : %.2f ％' % maxreturn)
    print('每次換手最少報酬 : %.2f ％' % minreturn)

    if benchmark is None:
        benchmark = price['0050'][start_date:end_date].iloc[1:]

    # bechmark (thanks to Markk1227)
    ((benchmark / benchmark[0] - 1) * 100).plot(ax=ax[0], legend=True, color=(0.8, 0.8, 0.8), grid=True)

    ax[0].set_ylabel('Return On Investment (%)')
    ax[0].grid(linestyle='-.')

    ((benchmark / benchmark.cummax() - 1) * 100).plot(ax=ax[1], legend=True, color=(0.8, 0.8, 0.8))
    ((equality / equality.cummax() - 1) * 100).plot(ax=ax[1], legend=True)
    plt.ylabel('Dropdown (%)')
    plt.grid(linestyle='-.')

    # pd.Series(nstock).plot.bar(ax=ax[2])
    # plt.ylabel('Number of stocks held')
    plt.show()

    return equality, transactions

def ExistFile():
    # target_folder = "D:\GOOGLE 雲端硬碟\Google 雲端硬碟\個人計畫追蹤\財報分析\測試資料夾"
    target_folder = 'D:\GOOGLE 雲端硬碟\Google 雲端硬碟\個人計畫追蹤\財報分析\台股'
    file = show_folder_content(target_folder, prefix="O_", postfix=".xlsx")

    index = []
    dictionary = {}
    for num in file[0:]:
        idx = ''.join([x for x in num if x.isdigit()])
        dictionary[idx] = num
        index.append(idx)
    return index

def SaveExcel(ID):

    path = "D:\GOOGLE 雲端硬碟\Google 雲端硬碟\個人計畫追蹤\財報分析\台股\樣板_財報分析.xlsx"
    wb = openpyxl.load_workbook(path)
    new_path = "D:\GOOGLE 雲端硬碟\Google 雲端硬碟\個人計畫追蹤\財報分析\台股\測選股結果\O_"+ ID +"_ooo財報分析.xlsx"
    wb.save(new_path)

'''
回測系統
'''
# 老師有建立回測系統，匯入
from finlab.backtest import backtest
import datetime

data = Data()

start = datetime.date(2021,4,1)
end = datetime.date(2021,5,22)
# date = start
# # 起始日期、結束日期、每幾天更換一次名單、選股策略、資料庫連結
# profit, record = backtest1(start, end, 31, data, stop_loss=-10, stop_profit=50)
# # profit , record = backtest(start, end, 30, mystrategy(data, end), data)
#
# print("交易利潤: ")
# print(profit)
# print("交易紀錄: ")
# print(record)

print("最新選股結果為: ")
# print(mystrategy(data, end))
list = list(mystrategy(data, end).index)
print(list)
# Exist = ExistFile()
# print(Exist)
# for id in list:
#     if id in Exist:
#         print(id, "已存在")
#     else:
#         print("新增", id)
#         # SaveExcel(id)

'''
投資組合內容


from finlab.backtest import portfolio

Stock_portfolio = mystrategy(data)
print(Stock_portfolio)
# portfolio function: 股票組合、擁有的資產額、資料庫連結、最低手續費、券商下單折扣、是否增加成本(add_cost)
p, total_investment_money = portfolio(
    Stock_portfolio.index, 3000000, data, lowest_fee=20, discount=0.6, add_cost=10
)

# 印出股票資訊
print('---------------')
print('|  portfolio  |')
print('---------------')
print(p)
print('total cost')
print(total_investment_money)

'''