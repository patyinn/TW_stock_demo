import os
import re
import math
import json
import sqlite3
import datetime
import operator
import requests
import asyncio
import threading
import numpy as np
import pandas as pd

from io import StringIO
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from openpyxl.styles import Border
from openpyxl.styles import Font
from openpyxl.styles import PatternFill
from openpyxl.styles import Side

from finlab.crawler_module import Crawler, CrawlerConnection
from finlab.data_module import RetrieveDataModule


class SystemProcessor:
    lock = threading.Lock()

    def __init__(self, sys_path):
        self.sys_path = sys_path
        self._check_file_existed()

    def _check_file_existed(self):
        if not os.path.exists(os.path.dirname(self.sys_path)):
            os.makedirs(os.path.dirname(self.sys_path))
        if not os.path.exists(self.sys_path):
            with open(self.sys_path, "w") as f:
                f.write(json.dumps({"path": {}, "condition": {}}, ensure_ascii=False, indent=4))

    def _write_to_json(self, table_name, key, value):
        with self.lock:
            with open(self.sys_path, "r+", encoding="UTF-8") as f:
                origin = json.load(f)
                if table_name == "path":
                    origin[table_name].setdefault(key, [])
                    origin[table_name][key].append(value)
                    origin[table_name][key] = list(set(origin[table_name][key]))
                elif table_name == "condition":
                    origin[table_name].setdefault(key, {})
                    origin[table_name][key] = value

                f.seek(0)
                json.dump(origin, f, ensure_ascii=False, indent=4)
                f.truncate()

    def _read_from_json(self, table_name, key):
        with self.lock:
            with open(self.sys_path, "r", encoding="UTF-8") as f:
                data = json.load(f)
                return data[table_name].get(key, None)

    def _del_from_json(self, table_name, key, value):
        with self.lock:
            with open(self.sys_path, "r+", encoding="UTF-8") as f:
                origin = json.load(f)
                if value in origin[table_name].get(key, []):
                    origin[table_name][key].remove(value)
                f.seek(0)
                json.dump(origin, f, ensure_ascii=False, indent=4)
                f.truncate()

    def save_path_sql(self, path, source="origin"):
        key, value = None, None
        if os.path.exists(path):
            if os.path.isdir(path):
                if source == "origin":
                    key = "directory"
                    value = path
                elif source == "select_stock":
                    key = "select_stock_directory"
                    value = path
            elif os.path.isfile(path):
                if path.endswith(".db"):
                    key = "db"
                    value = path
                elif path.endswith(".xlsx"):
                    key = "file"
                    value = path
            else:
                print("it's an invalid path")
        if key and value:
            self._write_to_json("path", key, value)

    def get_latest_path_sql(self, category):
        result = self._read_from_json("path", category)
        return result[-1] if result else ""

    def del_path_sql(self, category, path):
        self._del_from_json("path", category, path)

    def save_select_stock_cache_to_sql(self, combination):
        for com in zip(*combination):
            cond_dic = {
                "cond_name": com[0],
                "activate": com[1],
                "cond_content": com[2],
                "operator": com[3],
                "cond_value": com[4]
            }
            self._write_to_json("condition", cond_dic["cond_name"], cond_dic)

    def get_select_stock_cache_to_sql(self, condition):
        result = self._read_from_json("condition", condition)
        return result if result is not None else {
            "cond_name": condition,
            "activate": False,
            "cond_content": "",
            "operator": "",
            "cond_value": ""
        }

    # 把列出資料夾的程式碼寫成一個函式
    @classmethod
    def show_folder_content(cls, folder_path, prefix=None, postfix=None):
        files_list = []
        folder_content = os.listdir(folder_path)
        for item in folder_content:
            fullpath = os.path.join(folder_path, item)
            if os.path.isdir(fullpath):
                files_list += cls.show_folder_content(fullpath, prefix=prefix, postfix=postfix)
            elif os.path.isfile(fullpath):
                if prefix:
                    if item.startswith(prefix):
                        files_list.append(os.path.join(folder_path, item))
                elif postfix:
                    if item.endswith(postfix):
                        files_list.append(os.path.join(folder_path, item))
                else:
                    files_list.append(os.path.join(folder_path, item))
        return files_list


class CrawlerProcessor(Crawler):
    def __init__(self, conn, msg_queue):
        super().__init__(conn, msg_queue)
        self.conn = conn
        self.msg_queue = msg_queue

    async def exec_func(self, table, from_date, to_date, force=False):
        additional_arg = {}
        if table == "price":
            date = self.date_range(from_date, to_date)
            function = self.crawl_price
        elif table == "monthly_revenue":
            date = self.month_range(from_date, to_date)
            function = self.crawl_monthly_report
        elif table == "finance_statement":
            date = self.season_range(from_date, to_date)
            function = self.determine_crawl_finance_statement_func_by_date
            additional_arg = {
                "force": force,
                "base_directory": "",
            }
        await self.update_table(table, function, date, **additional_arg)

    def date_func(self, table, pattern):
        if table == "finance_statement":
            table = "balance_sheet"
        if pattern == "from":
            latest_date = self.table_latest_date(table)
            date_list = latest_date + datetime.timedelta(days=1)
            date_list = date_list.strftime('%Y-%m-%d')
        else:
            date_list = datetime.datetime.now().strftime('%Y-%m-%d')
        return [date_list]


class FinancialAnalysis(RetrieveDataModule, CrawlerConnection):
    def __init__(self, db_path, msg_queue, file_path):
        self.file_path = file_path
        self.wb = load_workbook(self.file_path)
        self.ws0 = self.wb["月財報"]
        self.ws1 = self.wb["季財報"]
        self.ws2 = self.wb["現金流量"]
        self.ws3 = self.wb["進出場參考"]
        self.ws4 = self.wb["合理價推估"]
        self.msg_queue = msg_queue
        conn = sqlite3.connect(db_path)
        super().__init__(conn, msg_queue)

    @staticmethod
    def season_transform(date, spec=None):
        if date is not pd.Series(dtype='object'):
            date = pd.Series(date)
        df = pd.DataFrame()
        df['Quarter'] = pd.to_datetime(date)
        df['Quarter'] = df['Quarter'].dt.to_period('Q').dt.strftime("%YQ%q")
        if spec:
            return df['Quarter']
        else:
            return df['Quarter'].iloc[-1]

    @staticmethod
    def season_determination(date):
        year = date.year
        if date.month <= 3:
            season = 4
            year = year - 1
        elif date.month <= 5:
            season = 1
        elif date.month <= 8:
            season = 2
        elif date.month <= 11:
            season = 3
        elif date.month <= 12:
            season = 4
        else:
            print("Wrong month to determine")
        return f"{year}Q{season}"

    @staticmethod
    def diff_months(str1, str2):
        year1 = datetime.datetime.strptime(str1[0:10], "%Y-%m").year
        year2 = datetime.datetime.strptime(str2[0:10], "%Y-%m").year
        month1 = datetime.datetime.strptime(str1[0:10], "%Y-%m").month
        month2 = datetime.datetime.strptime(str2[0:10], "%Y-%m").month
        num = (year1 - year2) * 12 + (month1 - month2)
        return num

    @classmethod
    def delta_seasons(cls, date, delta):
        str1 = cls.season_determination(date)
        year = int(str1[0:4])
        s = int(str1[-1])

        season = s - delta
        while season <= 0:
            season += 4
            year -= 1
        while season >= 5:
            season -= 4
            year += 1

        if season == 4:
            month = 3
            day = 31
            year += 1
        elif season == 3:
            month = 11
            day = 14
        elif season == 2:
            month = 8
            day = 14
        elif season == 1:
            month = 5
            day = 15
        else:
            print("Wrong season")
        return datetime.datetime(year, month, day)

    @staticmethod
    def warning_func(use_cond, sheet=None, rows=None, cols=None, threat=None):
        if use_cond:
            if threat:
                sheet.cell(row=rows, column=cols).font = Font(color='FF0000', bold=True)  # 紅色
                sheet.cell(row=rows, column=cols).fill = PatternFill(fill_type="solid", fgColor="FFFFBB")
                side_style = Side(style="thin", color="FF0000")
                sheet.cell(row=rows, column=cols).border = Border(left=side_style, right=side_style, top=side_style,
                                                                  bottom=side_style)
                sheet.cell(row=rows, column=1).fill = PatternFill(fill_type="solid", fgColor="AA0000")  # 深紅色
            else:
                sheet.cell(row=rows, column=cols).font = Font(color='FF0000', bold=False)  # 紅色
                sheet.cell(row=rows, column=cols).fill = PatternFill(fill_type="solid", fgColor="FFFFBB")
                sheet.cell(row=rows, column=1).fill = PatternFill(fill_type="solid", fgColor="FFAA33")  # 橘色
        else:
            sheet.cell(row=rows, column=cols).font = Font(color='000000')  # 黑色
            sheet.cell(row=rows, column=1).fill = PatternFill(fill_type="solid", fgColor="FFFFFF")  # 白色

    @staticmethod
    def _estimate_roe(c_roe):
        if c_roe.name.month == 5:
            return c_roe * 4
        elif c_roe.name.month == 8:
            return c_roe * 2
        elif c_roe.name.month == 11:
            return c_roe * 4 / 3
        return c_roe

    def write_to_excel(self, data, round_num=None, sheet=None, rows=None, cols=None, string="", date=None):
        if round_num:
            data = round(data, round_num)
        sheet.cell(row=rows, column=cols).value = data
        sheet.cell(row=rows, column=cols).alignment = Alignment(horizontal="center", vertical="center",
                                                                wrap_text=True)
        if string:
            msg = f"新增{date}的{string}: {data}"
            self.msg_queue.put(msg)
            print(msg)

    @staticmethod
    def get_cash_flow(raw_data):
        # 抓每年的Q4與最後一筆
        q4_data = raw_data[raw_data.index.month == 3]
        df_data = pd.concat([q4_data, raw_data.iloc[-1:]]).drop_duplicates()
        df_data.index = [datetime.datetime(idx.year - 1, idx.month, 1) if idx.month == 3 else idx for idx in df_data.index]
        return df_data

    async def _update_monthly_report(self, stock_id, path=None):
        '''    從資料庫獲取月營收最新日期    '''
        revenue_month = self.get_data('當月營收', 2)

        '''    時間判斷    '''
        # 改成用資料庫的最新時間尤佳
        latest_date = revenue_month[stock_id].dropna().index[-1]
        latest_date_str = datetime.datetime.strftime(latest_date, '%Y-%m')
        table_month = datetime.datetime.strftime(self.ws0["A5"].value, '%Y-%m')

        if table_month == latest_date_str:
            self.msg_queue.put("No data need to update.")
            print("No data need to update.")
        else:
            add_row_num = self.diff_months(latest_date_str, table_month)

            '''        根據相差月份取相對應數量的資料        '''
            add_revenue = add_row_num + 24
            revenue_month = self.get_data('當月營收', add_revenue) * 0.00001
            add_price = add_row_num * 40
            price = self.get_data('收盤價', add_price)
            mr_month_growth = self.get_data('上月比較增減(%)', add_revenue)
            mr_year_growth = self.get_data('去年同月增減(%)', add_revenue)

            # 輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票
            month_revenue = revenue_month[stock_id]
            price = price[stock_id]
            mr_month_growth = mr_month_growth[stock_id]
            mr_year_growth = mr_year_growth[stock_id]

            mag_3_m = mr_year_growth.rolling(3).mean().reindex(index=mr_year_growth.index)
            mag_3_m = round(mag_3_m, 2)
            mag_12_m = mr_year_growth.rolling(12).mean().reindex(index=mr_year_growth.index)
            mag_12_m = round(mag_12_m, 2)

            add_row_num -= 1
            for add_row in range(add_row_num, -1, -1):
                self.ws0.insert_rows(5, amount=1)

                '''  新增月份  '''
                update_month = latest_date - relativedelta(months=add_row)
                self.ws0.cell(row=5, column=1).value = update_month
                self.ws0.cell(row=5, column=1).number_format = "mmm-yy"
                self.ws0.cell(row=5, column=1).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                print(f"新增: {self.ws0.cell(row=5, column=1).value}")
                self.msg_queue.put(f"新增: {self.ws0.cell(row=5, column=1).value}")

                '''        更新營收        '''
                mr = round(month_revenue.loc[update_month], 2)
                self.ws0.cell(row=5, column=2).value = mr
                self.ws0.cell(row=5, column=2).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                print(f"新增月份: {update_month}的月營收: {mr}")
                self.msg_queue.put(f"新增月份: {update_month} 的月營收: {mr}")

                '''        更新月增率        '''
                mr_mg = round(mr_month_growth.loc[update_month], 2)
                self.ws0.cell(row=5, column=3).value = mr_mg
                self.ws0.cell(row=5, column=3).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                if mr_mg >= 0:
                    self.ws0.cell(row=5, column=3).font = Font(color='FF0000')  # 紅色
                else:
                    self.ws0.cell(row=5, column=3).font = Font(color='00FF00')  # 綠色

                print(f"新增{update_month}的月增率: {mr_mg}")
                self.msg_queue.put(f"新增{update_month}的月增率: {mr_mg}")

                '''        更新年增率        '''
                mr_yg = round(mr_year_growth.loc[update_month], 2)
                self.ws0.cell(row=5, column=4).value = mr_yg
                self.ws0.cell(row=5, column=4).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                if mr_yg >= 0:
                    self.ws0.cell(row=5, column=4).font = Font(color='FF0000')  # 紅色
                else:
                    self.ws0.cell(row=5, column=4).font = Font(color='00FF00')  # 綠色

                print(f"新增 {update_month} 的年增率: {round(mr_yg, 2)}")
                self.msg_queue.put(f"新增 {update_month} 的年增率: {round(mr_yg, 2)}")

                '''        更新當月最高、最低、平均收盤價        '''
                update_month_str = update_month.strftime('%Y-%m')
                self.ws0.cell(row=5, column=6).value = round(price.loc[update_month_str].max(), 2)
                self.ws0.cell(row=5, column=6).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                self.ws0.cell(row=5, column=7).value = round(price.loc[update_month_str].mean(), 2)
                self.ws0.cell(row=5, column=7).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                self.ws0.cell(row=5, column=8).value = round(price.loc[update_month_str].min(), 2)
                self.ws0.cell(row=5, column=8).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)

                '''        更新長、短期年增        '''
                self.ws0.cell(row=5, column=19).value = mag_3_m.loc[update_month]
                self.ws0.cell(row=5, column=19).alignment = Alignment(horizontal="center", vertical="center",
                                                                      wrap_text=True)
                self.ws0.cell(row=5, column=20).value = mag_12_m.loc[update_month]
                self.ws0.cell(row=5, column=20).alignment = Alignment(horizontal="center", vertical="center",
                                                                      wrap_text=True)
        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 月報".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 月報".format(stock_id))

    async def update_monthly_report(self, stock_id, path=None):
        '''    從資料庫獲取月營收最新日期    '''
        revenue_month = self.get_data('當月營收', 2)

        '''    時間判斷    '''
        # 改成用資料庫的最新時間尤佳
        latest_date = revenue_month[stock_id].dropna().index[-1]
        latest_date_str = datetime.datetime.strftime(latest_date, '%Y-%m')
        table_month = datetime.datetime.strftime(self.ws0["A5"].value, '%Y-%m')

        if table_month == latest_date_str:
            self.msg_queue.put("No month data need to update.")
            print("No month data need to update.")
        else:
            add_row_num = self.diff_months(latest_date_str, table_month)

            '''        根據相差月份取相對應數量的資料        '''
            add_revenue = add_row_num + 24
            target_cols = ['當月營收', '上月比較增減(%)', '去年同月增減(%)']
            mapper, dfs = self.get_bundle_data(target_cols, add_revenue, stock_id)
            df = pd.concat(dfs, axis=1)
            df['當月營收'] = df['當月營收'].multiply(0.00001)  # 單位: 億
            df = df.apply(lambda s: round(s, 2))

            add_price = add_row_num * 40
            price = self.get_data('收盤價', add_price)
            price = price[stock_id]

            # 計算3個月以及12個月的移動平均數
            df["3個月移動平均年增率"] = round(df["去年同月增減(%)"].rolling(3).mean(), 2)
            df["12個月移動平均年增率"] = round(df["去年同月增減(%)"].rolling(12).mean(), 2)

            target_cols.extend(["3個月移動平均年增率", "12個月移動平均年增率"])
            target_pos = [
                (5, 2, "月營收"),
                (5, 3, "月增率"),
                (5, 4, "年增率"),
                (5, 19, "短期平均年增率"),
                (5, 20, "長期平均年增率"),
            ]

            add_row_num -= 1
            for add_row in range(add_row_num, -1, -1):
                self.ws0.insert_rows(5, amount=1)

                '''  新增月份  '''
                update_month = latest_date - relativedelta(months=add_row)

                self.write_to_excel(update_month, sheet=self.ws0, rows=5, cols=1, string="月份標籤", date=f"{update_month}")
                self.ws0.cell(row=5, column=1).number_format = "mmm-yy"

                '''        更新營收        '''
                for col, pos in zip(target_cols, target_pos):
                    self.write_to_excel(
                        df.loc[update_month, col],
                        sheet=self.ws0,
                        rows=pos[0],
                        cols=pos[1],
                        string=pos[2],
                        date=f"{update_month}"
                    )
                    if col in ['上月比較增減(%)', '去年同月增減(%)']:
                        self.warning_func(
                            df.loc[update_month, col] >= 0,
                            sheet=self.ws0,
                            rows=pos[0],
                            cols=pos[1],
                            threat=False
                        )

                '''        更新當月最高、最低、平均收盤價        '''
                update_month_str = update_month.strftime('%Y-%m')
                self.write_to_excel(
                    round(price.loc[update_month_str].max(), 2), sheet=self.ws0, rows=5, cols=6,
                    string="最高股價", date=f"{update_month_str}"
                )
                self.write_to_excel(
                    round(price.loc[update_month_str].mean(), 2), sheet=self.ws0, rows=5, cols=7,
                    string="平均股價", date=f"{update_month_str}"
                )
                self.write_to_excel(
                    round(price.loc[update_month_str].min(), 2), sheet=self.ws0, rows=5, cols=8,
                    string="最低股價", date=f"{update_month_str}"
                )

            self.wb.save(path or self.file_path)
            print("完成更新 {} 的 月報".format(stock_id))
            self.msg_queue.put("完成更新 {} 的 月報".format(stock_id))

    async def _update_directors_and_supervisors(self, stock_id, path=None):
        # 設定headers
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
        }

        url = "https://goodinfo.tw/StockInfo/StockDirectorSharehold.asp?STOCK_ID=" + str(stock_id)
        r = requests.get_data(url, headers=headers)
        r.encoding = "utf-8"

        dfs = pd.read_html(StringIO(r.text))
        df = pd.concat([df for df in dfs if df.shape[1] > 15 and df.shape[0] > 30])
        idx = pd.IndexSlice
        df = df.loc[idx[:], idx[["月別", "全體董監持股"], :]]
        df.columns = df.columns.get_level_values(1)
        df = df.set_index(["月別"])
        df.columns = df.columns.str.replace(' ', '')

        df["持股(%)"] = pd.to_numeric(df["持股(%)"], errors="coerce")
        df = df[~ df["持股(%)"].isnull()].dropna()["持股(%)"]

        def change_name(string):
            dt_obj = datetime.datetime.strptime(string, '%Y/%m')
            dt_str = datetime.datetime.strftime(dt_obj, '%Y-%m')
            return dt_str

        df = df.rename(index=lambda s: change_name(s))
        data = []
        index = []
        for cell in list(self.ws0.columns)[9]:
            data.append(cell.value)
        data = data[4:]
        for cell in list(self.ws0.columns)[0]:
            index.append(cell.value)
        index = index[4:]

        data_now = pd.DataFrame({'date': index, 'Data': data})
        data_now = data_now[data_now['date'].notnull()].rename(index=lambda s: s + 5)

        # 確認爬蟲到的最新資料是否與excel的資料時間點相同，沒有就刪除excel資料點
        while datetime.datetime.strftime(data_now['date'].iloc[0], "%Y-%m") != df.index[0]:
            data_now = data_now.drop(data_now.index[0])
        update_data = data_now[data_now['Data'].isnull()]

        pd.options.mode.chained_assignment = None

        for n in range(len(update_data)):
            date = update_data['date'].iloc[n]
            date_str = datetime.datetime.strftime(date, "%Y-%m")
            try:
                update_data['Data'].iloc[n] = df.loc[date_str]
                r = update_data.index[n]
                if self.ws0.cell(row=r, column=1).value == date:
                    self.ws0.cell(row=r, column=10).value = update_data['Data'].iloc[n]
                    self.ws0.cell(row=r, column=10).alignment = Alignment(horizontal="center", vertical="center",
                                                                          wrap_text=True)
                    print(f"更新月份: {date_str} 的股東占比: {str(self.ws0.cell(row=r, column=10).value)}")
                    self.msg_queue.put(f"更新月份: {date_str} 的股東占比: {str(self.ws0.cell(row=r, column=10).value)}")
            except Exception as e:
                print(f"Doesn't get {date_str} Data")
                self.msg_queue.put(f"Doesn't get {date_str} Data")

        self.wb.save(path or self.file_path)
        await asyncio.sleep(1)
        print("完成更新 {} 的 股東占比".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 股東占比".format(stock_id))

    async def update_directors_and_supervisors(self, stock_id, path=None):
        url = "https://goodinfo.tw/StockInfo/StockDirectorSharehold.asp?STOCK_ID={}".format(str(stock_id))
        r = await self.requests_get(url)

        dfs = pd.read_html(StringIO(r.text))
        df = pd.concat([df for df in dfs if df.shape[1] > 15 and df.shape[0] > 30])
        idx = pd.IndexSlice
        df = df.loc[idx[:], idx[["月別", "全體董監持股"], :]]
        df.columns = df.columns.get_level_values(1)
        df = df.set_index(["月別"])
        df.columns = df.columns.str.replace(' ', '')

        df["持股(%)"] = pd.to_numeric(df["持股(%)"], errors="coerce")
        df = df[~ df["持股(%)"].isnull()].dropna()["持股(%)"]

        df = df.rename(index=lambda s: s.replace("/", "-"))
        data, index = [], []
        for cells in list(self.ws0.columns):
            data.append(cells[9].value)
            index.append(cells[0].value)
        data = data[4:]
        index = index[4:]

        data_now = pd.DataFrame({'date': index, 'Data': data})
        data_now = data_now[data_now['date'].notnull()].rename(index=lambda s: s + 5)

        # 確認爬蟲到的最新資料是否與excel的資料時間點相同，沒有就刪除excel資料點
        while datetime.datetime.strftime(data_now['date'].iloc[0], "%Y-%m") != df.index[0]:
            data_now = data_now.drop(data_now.index[0])
        update_data = data_now[data_now['Data'].isnull()]

        pd.options.mode.chained_assignment = None

        for n in range(len(update_data)):
            date = update_data['date'].iloc[n]
            date_str = datetime.datetime.strftime(date, "%Y-%m")
            try:
                update_data['Data'].iloc[n] = df.loc[date_str]
                r = update_data.index[n]
                if self.ws0.cell(row=r, column=1).value == date:
                    self.write_to_excel(
                        update_data['Data'].iloc[n],
                        sheet=self.ws0,
                        rows=r,
                        cols=10,
                        string="股東占比",
                        date=date_str
                    )
            except Exception as e:
                print(f"Doesn't get {date_str} Data")
                self.msg_queue.put(f"Doesn't get {date_str} Data")

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 股東占比".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 股東占比".format(stock_id))

    async def _update_season_report(self, stock_id, path=None):
        '''    從資料庫獲取季報最新日期    '''
        revenue_season = self.get_data_assign_table('營業收入合計', 5)
        revenue_season = revenue_season[stock_id]

        '''    時間判斷    '''
        # 改成用資料庫的最新時間尤佳
        latest_date = revenue_season.dropna().index[-1]
        latest_date_str = self.season_determination(latest_date)
        table_month = self.ws1["E1"].value
        add_column_num = 4 * (int(latest_date_str[0:4]) - int(table_month[0:4])) + (
                int(latest_date_str[-1]) - int(table_month[-1]))

        if add_column_num <= 0:
            print("No data need to update.")
            self.msg_queue.put("No data need to update.")
        else:
            '''        根據相差月份取相對應數量的資料        '''
            get_data_num = add_column_num + 6
            revenue_season = self.get_data_assign_table('營業收入合計', get_data_num) * 0.00001  # 單位: 億
            # 營業利益率，也可以簡稱營益率，英文Operating Margin或Operating profit Margin
            opm_raw = self.get_data_assign_table('營業利益（損失）', get_data_num) * 0.00001  # 單位: 億
            gross_profit = self.get_data_assign_table('營業毛利（毛損）', get_data_num) * 0.00001  # 單位: 億
            equity = self.get_data_assign_table("股本合計", get_data_num) * 0.00001  # 單位: 億
            profit_before_tax = self.get_data_assign_table("繼續營業單位稅前淨利（淨損）",
                                                           get_data_num) * 0.00001  # 單位: 億  本期稅前淨利（淨損）
            profit_after_tax = self.get_data_assign_table("本期淨利（淨損）", get_data_num) * 0.00001  # 單位: 億
            operating_costs = self.get_data_assign_table("營業成本合計", get_data_num) * 0.00001  # 單位: 億
            account_receivable = self.get_data_assign_table("應收帳款淨額", get_data_num) * 0.00001  # 單位: 億
            inventory = self.get_data_assign_table("存貨", get_data_num) * 0.00001  # 單位: 億
            assets = self.get_data_assign_table("資產總計", get_data_num) * 0.00001  # 單位: 億
            liabilities = self.get_data_assign_table("負債總計", get_data_num) * 0.00001  # 單位: 億
            accounts_payable = self.get_data_assign_table("應付帳款", get_data_num) * 0.00001  # 單位: 億
            intangible_assets = self.get_data_assign_table("無形資產", get_data_num) * 0.00001  # 單位: 億
            depreciation = self.get_data_assign_table("折舊費用", get_data_num, table="cash_flows") * 0.00001  # 單位: 億
            net_income = self.get_data_assign_table('本期淨利（淨損）', get_data_num) * 0.00001  # 單位: 億
            # 修正：因為有些股東權益的名稱叫作「權益總計」有些叫作「權益總額」，所以要先將這兩個dataframe合併起來喔！
            shareholders_equity = self.get_data_assign_table('權益總計', get_data_num)
            total_equity = self.get_data_assign_table('權益總額', get_data_num)
            # 把它們合併起來（將「權益總計」為NaN的部分填上「權益總額」）
            shareholders_equity.fillna(total_equity, inplace=False) * 0.00001  # 單位: 億

            price_num = add_column_num * 65
            price = self.get_data_assign_table("收盤價", price_num)

            '''        輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票        '''
            revenue_season = revenue_season[stock_id]
            opm_raw = opm_raw[stock_id]
            gross_profit = gross_profit[stock_id]
            equity = equity[stock_id]
            price = price[stock_id]
            profit_before_tax = profit_before_tax[stock_id]
            profit_after_tax = profit_after_tax[stock_id]
            operating_costs = operating_costs[stock_id]
            account_receivable = account_receivable[stock_id]
            inventory = inventory[stock_id]
            assets = assets[stock_id]
            liabilities = liabilities[stock_id]
            accounts_payable = accounts_payable[stock_id]
            intangible_assets = intangible_assets[stock_id]
            depreciation = depreciation[stock_id]
            net_income = net_income[stock_id]
            shareholders_equity = shareholders_equity[stock_id]

            '''        拆解數據處理        '''
            d_depreciation = self.data_process(depreciation, cum=False)
            '''        累積數據處理        '''
            c_return_on_equity = net_income / shareholders_equity * 100
            c_return_on_equity = self.data_process(c_return_on_equity, cum=True)

            c_profit_after_tax = self.data_process(profit_after_tax, cum=True)
            c_revenue_season = self.data_process(revenue_season, cum=True)
            c_profit_after_tax = c_profit_after_tax / c_revenue_season * 100

            c_shareholders_equity = shareholders_equity / assets * 100

            new_assets = []
            for idx in range(len(assets)):
                new_assets.append((assets[idx] + assets[idx - 1]) / 2)
            new_assets = pd.Series(new_assets, index=assets.index)
            new_assets = new_assets.drop(labels=[assets.index[0]])
            c_new_assets = self.data_process(new_assets, cum=True)
            c_total_assets_turnover = c_revenue_season / c_new_assets * 4

            add_column_num *= -1

            for add_row in range(add_column_num, 0, 1):

                self.ws1.insert_cols(5, amount=1)

                update_season_date = revenue_season.index[add_row]
                update_season_str = update_season_date.strftime('%Y-%m-%d')
                season_last_year = self.delta_seasons(update_season_date, 4)
                season_prev4_season = self.delta_seasons(update_season_date, 3)
                season_prev_season = self.delta_seasons(update_season_date, 1)

                '''  新增季度標籤  '''
                update_season = self.season_determination(update_season_date)

                self.ws1.cell(row=1, column=5).value = update_season
                self.ws1.cell(row=1, column=5).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                self.ws1.cell(row=1, column=5).fill = PatternFill(fill_type="solid", fgColor="DDDDDD")
                print(f"新增標籤: {self.ws1.cell(row=1, column=5).value}")
                self.msg_queue.put(f"新增標籤: {self.ws1.cell(row=1, column=5).value}")

                '''  新增當期營收、當期營收年成長率  '''
                sr = revenue_season.loc[update_season_date]
                sr_4 = revenue_season.loc[season_last_year]
                s_revenue_yg = (sr - sr_4) / sr_4 * 100

                self.write_to_excel(sr, round_num=2, sheet=self.ws1, rows=3, cols=5, string="當季營收",
                                    date=update_season_str)
                self.write_to_excel(s_revenue_yg, round_num=2, sheet=self.ws1, rows=4, cols=5, string="年增率",
                                    date=update_season_str)

                '''   營業毛利率   '''
                gp = gross_profit.loc[update_season_date] / sr * 100

                self.write_to_excel(gp, round_num=2, sheet=self.ws1, rows=6, cols=5, string="營業毛利率",
                                    date=update_season_str)

                '''   營業利益率、營業利益成長率   '''
                opm = opm_raw.loc[update_season_date] / sr * 100
                opm_1 = opm_raw.loc[season_prev_season] / revenue_season.loc[season_prev_season] * 100
                opm_sg = (opm - opm_1) / opm_1 * 100

                self.write_to_excel(opm, round_num=2, sheet=self.ws1, rows=7, cols=5, string="營業利益率",
                                    date=update_season_str)
                self.write_to_excel(opm_sg, round_num=2, sheet=self.ws1, rows=8, cols=5, string="營業利益成長率",
                                    date=update_season_str)

                '''   新增股本、股本季增率、當期市值與市值營收比   '''
                price_eq = price.loc[:update_season_date].iloc[-1]  # 確認股本公布當天是否為交易日
                equity_eq = equity.loc[update_season_date]  # 取得最新一筆的股本
                equity_eq_1 = equity.loc[season_prev_season]

                equity_eq_sg = (equity_eq - equity_eq_1) / equity_eq_1 * 100
                market_value = price_eq * equity_eq / 10  # 市值 = 股價 * 總股數 (股本合計單位為 k元)
                psr = revenue_season.loc[season_prev4_season: update_season_date].sum() / market_value * 100

                self.write_to_excel(equity_eq, round_num=0, sheet=self.ws1, rows=21, cols=5, string="股本",
                                    date=update_season_str)
                self.write_to_excel(equity_eq_sg, round_num=0, sheet=self.ws1, rows=22, cols=5, string="股本季增率",
                                    date=update_season_str)
                self.write_to_excel(market_value, round_num=0, sheet=self.ws1, rows=5, cols=5, string="市值",
                                    date=update_season_str)
                self.write_to_excel(psr, round_num=2, sheet=self.ws1, rows=19, cols=5, string="營收市值比",
                                    date=update_season_str)

                '''   新增稅前淨利率、本業收入比率、稅後淨利率、稅後淨利年增率  '''
                pbt = profit_before_tax.loc[update_season_date] / sr * 100
                revenue_source = opm / pbt
                pat = profit_after_tax.loc[update_season_date] / sr * 100
                pat_4 = profit_after_tax.loc[season_last_year]
                pat_yg = (profit_after_tax.loc[update_season_date] - pat_4) / pat_4 * 100

                self.write_to_excel(pbt, round_num=2, sheet=self.ws1, rows=9, cols=5, string="稅前淨利率",
                                    date=update_season_str)
                self.write_to_excel(revenue_source, round_num=2, sheet=self.ws1, rows=10, cols=5, string="本業收入比率",
                                    date=update_season_str)
                self.write_to_excel(pat, round_num=2, sheet=self.ws1, rows=11, cols=5, string="稅後淨利率",
                                    date=update_season_str)
                self.write_to_excel(pat_yg, round_num=2, sheet=self.ws1, rows=12, cols=5, string="稅後淨利年增率",
                                    date=update_season_str)

                '''   新增EPS、EPS年成長率   '''
                eps = profit_after_tax.loc[update_season_date] / (equity_eq / 10)
                eps_4 = pat_4 / (equity.loc[season_last_year] / 10)
                eps_yg = (eps - eps_4) / eps_4 * 100

                self.write_to_excel(eps, round_num=2, sheet=self.ws1, rows=13, cols=5, string="每股稅後盈餘",
                                    date=update_season_str)
                self.write_to_excel(eps_yg, round_num=2, sheet=self.ws1, rows=14, cols=5, string="每股稅後盈餘年成長率",
                                    date=update_season_str)

                '''   新增應收帳款週轉率、存貨周轉率、存貨營收比   '''
                ar = account_receivable.loc[update_season_date]
                ar_1 = account_receivable.loc[season_prev_season]
                # receivables turnover
                rt = sr / ((ar + ar_1) / 2) * 4

                oc = operating_costs.loc[update_season_date]
                inv = inventory.loc[update_season_date]
                inv_1 = inventory.loc[season_prev_season]
                # inventory turnover
                it = oc / ((inv + inv_1) / 2) * 4
                # inventory revenue ratio
                ir = inv / sr * 100

                self.write_to_excel(rt, round_num=2, sheet=self.ws1, rows=16, cols=5, string="應收帳款週轉率",
                                    date=update_season_str)
                self.write_to_excel(it, round_num=2, sheet=self.ws1, rows=17, cols=5, string="存貨周轉率",
                                    date=update_season_str)
                self.write_to_excel(ir, round_num=2, sheet=self.ws1, rows=18, cols=5, string="存貨占營收比",
                                    date=update_season_str)

                '''   新增應付帳款總資產占比、負債總資產占比、無形資產占比'''
                ass = assets.loc[update_season_date]
                lia = liabilities.loc[update_season_date]
                ap = accounts_payable.loc[update_season_date]
                int_a = intangible_assets.loc[update_season_date]

                lia_ratio = lia / ass * 100
                ap_ratio = ap / ass * 100
                int_a_ratio = int_a / ass * 100

                self.write_to_excel(ap_ratio, round_num=2, sheet=self.ws1, rows=23, cols=5,
                                    string="供應商應付帳款總資產占比",
                                    date=update_season_str)
                self.write_to_excel(lia_ratio, round_num=2, sheet=self.ws1, rows=24, cols=5, string="負債總資產占比",
                                    date=update_season_str)
                self.write_to_excel(int_a_ratio, round_num=2, sheet=self.ws1, rows=25, cols=5, string="無形資產占比",
                                    date=update_season_str)

                '''   新增折舊、折舊負擔比率'''
                dep = d_depreciation.loc[update_season_date]
                # Debt Asset ratio
                dar = dep / sr

                self.write_to_excel(dep, round_num=2, sheet=self.ws1, rows=27, cols=5, string="折舊",
                                    date=update_season_str)
                self.write_to_excel(dar, round_num=2, sheet=self.ws1, rows=28, cols=5, string="折舊負擔比率",
                                    date=update_season_str)

                '''   杜邦分析   '''
                c_roe = c_return_on_equity.loc[update_season_date]
                if update_season_date.month == 5:
                    ce_roe = c_roe * 4
                elif update_season_date.month == 8:
                    ce_roe = c_roe * 2
                elif update_season_date.month == 11:
                    ce_roe = c_roe * 4 / 3
                else:
                    ce_roe = c_roe
                c_tat = c_total_assets_turnover.loc[update_season_date]
                c_pat = c_profit_after_tax.loc[update_season_date]
                c_se = c_shareholders_equity.loc[update_season_date]
                # Equity Multiplier
                c_em = 1 / c_se * 100

                self.write_to_excel(c_roe, round_num=2, sheet=self.ws1, rows=30, cols=5, string="股東權益報酬率(季)",
                                    date=update_season_str)
                self.write_to_excel(ce_roe, round_num=2, sheet=self.ws1, rows=31, cols=5, string="股東權益報酬率(年預估)",
                                    date=update_season_str)
                self.write_to_excel(c_pat, round_num=2, sheet=self.ws1, rows=32, cols=5, string="稅後淨利率(累計)",
                                    date=update_season_str)
                self.write_to_excel(c_tat, round_num=2, sheet=self.ws1, rows=33, cols=5, string="總資產週轉率(次/年)",
                                    date=update_season_str)
                self.write_to_excel(c_em, round_num=2, sheet=self.ws1, rows=34, cols=5, string="權益係數",
                                    date=update_season_str)
                self.write_to_excel(c_se, round_num=2, sheet=self.ws1, rows=35, cols=5, string="股東權益總額(%)",
                                    date=update_season_str)

            self.wb.save(path or self.file_path)

        # 營收年成長率
        condition_sg = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E4':'L4']):
            for e, a in zip(date, data1):
                condition_sg[e.value] = a.value
        condition_sg = condition_sg.fillna(0) < 0

        # 營收利益成長率
        condition_opm_sg2 = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E8':'L8']):
            for e, a in zip(date, data1):
                condition_opm_sg2[e.value] = a.value
        condition_opm_sg2 = condition_opm_sg2 < -30

        # 營收利益成長率
        condition_opm_sg = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E8':'L8']):
            for e, a in zip(date, data1):
                condition_opm_sg[e.value] = a.value
        condition_opm_sg = condition_opm_sg.between(-30, -20)

        # 營收市值比
        condition_psr = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E19':'L19']):
            for e, a in zip(date, data1):
                condition_psr[e.value] = a.value
        condition_psr = condition_psr.fillna(0) < 20

        # EPS年成長率
        condition_eps_yg = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E17':'L17']):
            for e, a in zip(date, data1):
                condition_eps_yg[e.value] = a.value
        condition_eps_yg = condition_eps_yg.fillna(0) < 0

        # 負債總額
        condition_lia_ratio = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E24':'L24']):
            for e, a in zip(date, data1):
                condition_lia_ratio[e.value] = a.value
        condition_lia_ratio = condition_lia_ratio.fillna(0) > 40

        # 無形資產占比
        condition_int_a_ratio1 = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E25':'L25']):
            for e, a in zip(date, data1):
                condition_int_a_ratio1[e.value] = a.value
        condition_int_a_ratio1 = condition_int_a_ratio1.fillna(0) > 10

        # 無形資產占比
        condition_int_a_ratio = pd.Series([], dtype=pd.StringDtype())
        for date, data1 in zip(self.ws1['E1':'L1'], self.ws1['E25':'L25']):
            for e, a in zip(date, data1):
                condition_int_a_ratio[e.value] = a.value
        condition_int_a_ratio = condition_int_a_ratio.fillna(0) > 30

        # 折舊負擔比率
        condition_dar = pd.DataFrame()
        for date, data1, data2 in zip(self.ws1['E1':'L1'], self.ws1['E28':'L28'], self.ws1['E6':'L6']):
            for e, a1, a2 in zip(date, data1, data2):
                condition_dar[e.value] = [a1.value, a2.value]
        condition_dar = condition_dar.fillna(0).iloc[0] > condition_dar.fillna(0).iloc[1]

        '''   判斷條件   '''
        for c in range(5, 13):
            n = c - 5
            # 營收年成長率
            self.warning_func(condition_sg[n], sheet=self.ws1, rows=4, cols=c, threat='False')
            # 營收利益成長率
            self.warning_func(condition_opm_sg[n], sheet=self.ws1, rows=8, cols=c, threat='False')
            # 營收利益成長率
            self.warning_func(condition_opm_sg2[n], sheet=self.ws1, rows=8, cols=c, threat='True')
            # 營收市值比
            self.warning_func(condition_psr[n], sheet=self.ws1, rows=19, cols=c, threat='False')
            # EPS年成長率
            self.warning_func(condition_eps_yg[n], sheet=self.ws1, rows=17, cols=c, threat='False')
            # 負債總額
            self.warning_func(condition_lia_ratio[n], sheet=self.ws1, rows=24, cols=c, threat='False')
            # 無形資產占比
            self.warning_func(condition_int_a_ratio1[n], sheet=self.ws1, rows=25, cols=c, threat='False')
            self.warning_func(condition_int_a_ratio[n], sheet=self.ws1, rows=25, cols=c, threat='True')
            # 折舊負擔比率
            self.warning_func(condition_dar[n], sheet=self.ws1, rows=28, cols=c, threat='False')
        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 季報".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 季報".format(stock_id))

    async def update_season_report(self, stock_id, path=None):
        '''    從資料庫獲取季報最新日期    '''
        revenue_season = self.get_data_assign_table('營業收入合計', 5)
        revenue_season = revenue_season[stock_id]

        '''    時間判斷    '''
        # 改成用資料庫的最新時間尤佳
        latest_date = revenue_season.dropna().index[-1]
        latest_date_str = self.season_determination(latest_date)
        table_month = self.ws1["E1"].value
        add_column_num = 4 * (int(latest_date_str[0:4]) - int(table_month[0:4])) + (
                int(latest_date_str[-1]) - int(table_month[-1]))

        if add_column_num <= 0:
            print("No data need to update.")
            self.msg_queue.put("No data need to update.")
        else:
            '''        根據相差月份取相對應數量的資料        '''
            get_data_num = add_column_num + 6
            target_cols = [
                '營業收入合計',
                '營業利益（損失）',
                '營業毛利（毛損）',
                "股本合計",
                "繼續營業單位稅前淨利（淨損）",
                "本期淨利（淨損）",
                "營業成本合計",
                "應收帳款淨額",
                "存貨",
                "資產總計",
                "負債總計",
                "應付帳款",
                "無形資產",
                "折舊費用",
                '權益總計',
                '權益總額',
            ]
            mapper, dfs = self.get_bundle_data(target_cols, get_data_num, stock_id, assign_table={"折舊費用": "cash_flows"})
            df = pd.concat(dfs, axis=1)
            df = df.multiply(0.00001)  # 單位: 億
            df["權益總計"].fillna(df["權益總額"], inplace=True)
            df.drop("權益總額", axis=1, inplace=True)

            price_num = add_column_num * 65
            price = self.get_data_assign_table("收盤價", price_num)
            price = price[stock_id]
            season_report_price = price[price.index.isin(df.index)]

            '''        拆解數據處理        '''
            df["遞減折舊費用"] = df.loc[:, ["折舊費用"]].apply(lambda x: self.data_process(x, cum=False))
            '''        累積數據處理        '''
            df["累積股東權益報酬率(季)"] = self.data_process((df["本期淨利（淨損）"] / df["權益總計"] * 100), cum=True)
            df["累積季營收"] = self.data_process((df['營業收入合計']), cum=True)
            df["累積稅後淨利率"] = self.data_process((df["本期淨利（淨損）"]), cum=True) / df["累積季營收"] * 100
            df["累積營收淨值比"] = (df["本期淨利（淨損）"] / df["累積季營收"]) * 100
            df["累積股東權益資產轉換率"] = (df["權益總計"] / df["資產總計"]) * 100
            df["累積資產變化"] = self.data_process((df["資產總計"] + df["資產總計"].shift(1)) / 2, cum=True)
            df["總資產週轉率(次/年)"] = df["累積季營收"] / df["累積資產變化"] * 4

            '''        處理需要放到excel的資料        '''
            df["季營收年增率"] = 100 * (df["營業收入合計"] / df["營業收入合計"].shift(4)) - 100
            df["營業毛利率"] = 100 * (df["營業毛利（毛損）"] / df["營業收入合計"])
            df["營業利益率"] = 100 * (df["營業利益（損失）"] / df["營業收入合計"])
            df["營業利益成長率"] = 100 * (df["營業利益率"] / df["營業利益率"].shift(1)) - 100
            df["股本季增率"] = 100 * (df["股本合計"] / df["股本合計"].shift(1)) - 100
            df["市值"] = season_report_price * df["股本合計"] / 10  # 市值 = 股價 * 總股數 (股本合計單位為 k元)
            df["營收市值比"] = df["營業收入合計"].rolling(4).sum() / df["市值"] * 100
            df["稅前淨利率"] = 100 * (df["繼續營業單位稅前淨利（淨損）"] / df["營業收入合計"])
            df["本業收入比率"] = 100 * (df["營業利益（損失）"] / df["繼續營業單位稅前淨利（淨損）"])
            df["稅後淨利率"] = 100 * (df["本期淨利（淨損）"] / df["營業收入合計"])
            df["稅後淨利年增率"] = 100 * (df["稅後淨利率"] / df["稅後淨利率"].shift(4)) - 100
            df["每股稅後盈餘"] = df["本期淨利（淨損）"] / (df["股本合計"] / 10)
            df["每股稅後盈餘年成長率"] = 100 * (df["每股稅後盈餘"] / df["每股稅後盈餘"].shift(4)) - 100
            df["應收帳款週轉率"] = df["營業收入合計"] / ((df["應收帳款淨額"] + df["應收帳款淨額"].shift(1)) / 2) * 4
            df["存貨周轉率"] = df["營業成本合計"] / ((df["存貨"] + df["存貨"].shift(1)) / 2) * 4
            df["存貨占營收比"] = 100 * (df["存貨"] / df["營業收入合計"])
            df["折舊負擔比率"] = df["遞減折舊費用"] / df["營業收入合計"]
            df["供應商應付帳款總資產占比"] = 100 * (df["應付帳款"] / df["資產總計"])
            df["負債總資產占比"] = 100 * (df["負債總計"] / df["資產總計"])
            df["無形資產占比"] = 100 * (df["無形資產"] / df["資產總計"])
            df["股東權益報酬率(年預估)"] = df.loc[:, ["累積股東權益報酬率(季)"]].apply(self._estimate_roe, axis=1)
            df["權益係數"] = 100 / df["累積股東權益資產轉換率"]

            condition_df = pd.DataFrame()
            condition_df["季營收年增率"] = df["季營收年增率"] <= 0
            condition_df["營業利益成長率t"] = df["營業利益成長率"] < -30
            condition_df["營業利益成長率"] = df["營業利益成長率"].between(-30, -20)
            condition_df["營收市值比"] = df["營收市值比"] < 20
            condition_df["每股稅後盈餘年成長率"] = df["每股稅後盈餘年成長率"] < 0
            condition_df["負債總資產占比"] = df["負債總資產占比"] > 40
            condition_df["無形資產占比"] = df["無形資產占比"] > 10
            condition_df["無形資產占比t"] = df["無形資產占比"] > 30
            condition_df["折舊負擔比率"] = df["折舊負擔比率"] > df["營業利益率"]

            warning_on_excel = {
                "季營收年增率": [df["季營收年增率"] <= 0],
                "營業利益成長率": [df["營業利益成長率"] < -30, df["營業利益成長率"].between(-30, -20)],
                "營收市值比": [df["營收市值比"] < 20],
                "每股稅後盈餘年成長率": [df["每股稅後盈餘年成長率"] < 0],
                "負債總資產占比": [df["負債總資產占比"] > 40],
                "無形資產占比": [df["無形資產占比"] > 10, df["無形資產占比"] > 30],
                "折舊負擔比率": [df["折舊負擔比率"] > df["營業利益率"]],
            }

            write_df_to_excel = [
                ("營業收入合計", 3, 5, "當季營收",),
                ("季營收年增率", 4, 5, "季營收年增率",),
                ("營業毛利率", 6, 5, "營業毛利率",),
                ("營業利益率", 7, 5, "營業利益率",),
                ("營業利益成長率", 8, 5, "營業利益成長率",),
                ("股本合計", 21, 5, "股本",),
                ("股本季增率", 22, 5, "股本季增率",),
                ("市值", 5, 5, "市值",),
                ("營收市值比", 19, 5, "營收市值比",),
                ("稅前淨利率", 9, 5, "稅前淨利率",),
                ("本業收入比率", 10, 5, "本業收入比率",),
                ("稅後淨利率", 11, 5, "稅後淨利率",),
                ("稅後淨利年增率", 12, 5, "稅後淨利年增率",),
                ("每股稅後盈餘", 13, 5, "每股稅後盈餘",),
                ("每股稅後盈餘年成長率", 14, 5, "每股稅後盈餘年成長率",),
                ("應收帳款週轉率", 16, 5, "應收帳款週轉率",),
                ("存貨周轉率", 17, 5, "存貨周轉率",),
                ("存貨占營收比", 18, 5, "存貨占營收比",),
                ("遞減折舊費用", 27, 5, "折舊",),
                ("折舊負擔比率", 28, 5, "折舊負擔比率",),
                ("供應商應付帳款總資產占比", 23, 5, "供應商應付帳款總資產占比",),
                ("負債總資產占比", 24, 5, "負債總資產占比",),
                ("無形資產占比", 25, 5, "無形資產占比",),
                ("累積股東權益報酬率(季)", 30, 5, "股東權益報酬率(季)",),
                ("股東權益報酬率(年預估)", 31, 5, "股東權益報酬率(年預估)",),
                ("累積稅後淨利率", 32, 5, "稅後淨利率(累計)",),
                ("總資產週轉率(次/年)", 33, 5, "總資產週轉率(次/年)",),
                ("權益係數", 34, 5, "權益係數",),
                ("累積股東權益資產轉換率", 35, 5, "股東權益總額(%)",),
            ]

            add_column_num *= -1
            for add_row in range(add_column_num, 0, 1):
                self.ws1.insert_cols(5, amount=1)
                update_season_date = df["營業收入合計"].index[add_row]
                update_season_str = update_season_date.strftime('%Y-%m-%d')

                '''  新增季度標籤  '''
                update_season = self.season_determination(update_season_date)
                self.write_to_excel(update_season, sheet=self.ws1, rows=1, cols=5, string="季度標籤",
                                    date=f"{update_season}")
                self.ws1.cell(row=1, column=5).fill = PatternFill(fill_type="solid", fgColor="DDDDDD")

                for data in write_df_to_excel:
                    self.write_to_excel(
                        df.loc[update_season_date, data[0]],
                        sheet=self.ws1,
                        round_num=2,
                        rows=data[1],
                        cols=data[2],
                        string=data[3],
                        date=f"{update_season_str}"
                    )
                    if warning_on_excel.get(data[0]):
                        warning_cond = warning_on_excel[data[0]]
                        for i, cond in enumerate(warning_cond):
                            if cond[update_season_date] and i == 0:
                                self.warning_func(
                                    True,
                                    sheet=self.ws1,
                                    rows=data[1],
                                    cols=data[2],
                                    threat=True
                                )
                                break
                            elif cond[update_season_str] and i == 1:
                                self.warning_func(
                                    True,
                                    sheet=self.ws1,
                                    rows=data[1],
                                    cols=data[2],
                                    threat=False
                                )

            self.wb.save(path or self.file_path)
            print("完成更新 {} 的 季報".format(stock_id))
            self.msg_queue.put("完成更新 {} 的 季報".format(stock_id))

    async def _update_cash_flow(self, stock_id, path=None):
        '''    從資料庫獲取季報最新日期    '''
        cash_flow_for_investing = self.get_data_assign_table("投資活動之淨現金流入（流出）", 5)
        cash_flow_for_investing = cash_flow_for_investing[stock_id]

        '''    時間判斷    '''
        latest_date = cash_flow_for_investing.dropna().index[-1]
        if latest_date.month == 3:
            year = latest_date.year - 1
        else:
            year = latest_date.year
        table_year = self.ws2["D1"].value
        add_column_num = year - int(table_year)

        '''    確認當年資料是否需要更新    '''
        if self.ws2["D4"].value != cash_flow_for_investing[-1]:
            self.ws2.delete_cols(4, 1)
            print("當年度資料更新")
            self.msg_queue.put("當年度資料更新")
            add_column_num += 1

        if add_column_num <= 0:
            print("No data need to update.")
            self.msg_queue.put("No data need to update.")
        else:
            '''        根據相差月份取相對應數量的資料        '''
            get_data_num = add_column_num * 4
            # Cash Flow for investing
            cash_flow_for_investing = self.get_data_assign_table("投資活動之淨現金流入（流出）", get_data_num)
            # Operating Cash Flow
            operating_cash_flow = self.get_data_assign_table("營業活動之淨現金流入（流出）", get_data_num)
            # Cash Flows Provided from Financing Activities
            cash_flow_for_financing = self.get_data_assign_table("籌資活動之淨現金流入（流出）", get_data_num)
            # Cash Balances - Beginning of Period
            cash_balances_beginning = self.get_data_assign_table("期初現金及約當現金餘額", get_data_num)
            # Cash Balances - End of Period
            cash_balances_end = self.get_data_assign_table("期末現金及約當現金餘額", get_data_num)

            '''        輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票        '''
            cash_flow_for_investing = cash_flow_for_investing[stock_id] * 0.00001  # 單位:億
            operating_cash_flow = operating_cash_flow[stock_id] * 0.00001  # 單位:億
            # Free cash flow(FCF)
            free_cash_flow = (cash_flow_for_investing + operating_cash_flow)
            cash_flow_for_financing = cash_flow_for_financing[stock_id] * 0.00001  # 單位:億
            cash_balances_beginning = cash_balances_beginning[stock_id] * 0.00001  # 單位:億
            cash_balances_end = cash_balances_end[stock_id] * 0.00001  # 單位:億

            cash_flow_for_investing = self.get_cash_flow(cash_flow_for_investing)
            operating_cash_flow = self.get_cash_flow(operating_cash_flow)
            free_cash_flow = self.get_cash_flow(free_cash_flow)
            cash_flow_for_financing = self.get_cash_flow(cash_flow_for_financing)
            cash_balances_beginning = self.get_cash_flow(cash_balances_beginning)
            cash_balances_end = self.get_cash_flow(cash_balances_end)

            add_column_num *= -1

            for add_row in range(add_column_num, 0, 1):
                self.ws2.insert_cols(4, amount=1)

                update_year = cash_flow_for_investing.index[add_row]
                update_year_str = update_year.strftime('%Y')

                '''  新增年度標籤  '''
                self.ws2.cell(row=1, column=4).value = update_year_str
                self.ws2.cell(row=1, column=4).alignment = Alignment(horizontal="center", vertical="center",
                                                                     wrap_text=True)
                self.ws2.cell(row=1, column=4).fill = PatternFill(fill_type="solid", fgColor="DDDDDD")
                print(f"新增標籤: {self.ws2.cell(row=1, column=4).value}")
                self.msg_queue.put(f"新增標籤: {self.ws2.cell(row=1, column=4).value}")

                '''  新增營業活動現金、理財活動現金、自由現金流量、籌資活動現金'''
                icf = cash_flow_for_investing.loc[update_year]
                ocf = operating_cash_flow.loc[update_year]
                fcf = free_cash_flow.loc[update_year]
                cfpfa = cash_flow_for_financing.loc[update_year]

                self.write_to_excel(ocf, round_num=1, sheet=self.ws2, rows=3, cols=4, string="營業活動現金",
                                    date=update_year_str)
                self.write_to_excel(icf, round_num=1, sheet=self.ws2, rows=4, cols=4, string="理財活動現金",
                                    date=update_year_str)
                self.write_to_excel(fcf, round_num=1, sheet=self.ws2, rows=5, cols=4, string="自由現金流量",
                                    date=update_year_str)
                self.write_to_excel(cfpfa, round_num=1, sheet=self.ws2, rows=6, cols=4, string="籌資活動現金",
                                    date=update_year_str)

                self.write_to_excel(ocf, sheet=self.ws2, rows=3, cols=4)
                self.write_to_excel(icf, sheet=self.ws2, rows=4, cols=4)
                self.write_to_excel(fcf, sheet=self.ws2, rows=5, cols=4)
                self.write_to_excel(cfpfa, sheet=self.ws2, rows=6, cols=4)

                '''  新增期初現金及約當現金餘額、期末現金及約當現金餘額'''
                cbbp = cash_balances_beginning.loc[update_year]
                cbep = cash_balances_end.loc[update_year]

                self.write_to_excel(cbbp, round_num=1, sheet=self.ws2, rows=7, cols=4, string="期初現金及約當現金餘額",
                                    date=update_year_str)
                self.write_to_excel(cbep, round_num=1, sheet=self.ws2, rows=8, cols=4, string="期末現金及約當現金餘額",
                                    date=update_year_str)
        try:
            '''   判斷條件   '''
            for c in range(4, 9):
                # 營業活動現金
                condition_ocf = int(self.ws2.cell(row=3, column=c).value) < 0
                self.warning_func(condition_ocf, sheet=self.ws2, rows=3, cols=c, threat='True')
                # 自由現金
                condition_fcf = int(self.ws2.cell(row=5, column=c).value) < 0
                self.warning_func(condition_fcf, sheet=self.ws2, rows=5, cols=c, threat='True')
        except:
            print(f"{stock_id} 警告上色錯誤")
            self.msg_queue.put(f"{stock_id} 警告上色錯誤")

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 現金流量表".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 現金流量表".format(stock_id))

    async def update_cash_flow(self, stock_id, path=None):
        '''    從資料庫獲取季報最新日期    '''
        cash_flow_for_investing = self.get_data_assign_table("投資活動之淨現金流入（流出）", 5)
        cash_flow_for_investing = cash_flow_for_investing[stock_id]

        '''    時間判斷    '''
        latest_date = cash_flow_for_investing.dropna().index[-1]
        if latest_date.month == 3:
            year = latest_date.year - 1
        else:
            year = latest_date.year
        table_year = self.ws2["D1"].value
        add_column_num = year - int(table_year)

        '''    確認當年資料是否需要更新    '''
        if self.ws2["D4"].value != cash_flow_for_investing[-1]:
            self.ws2.delete_cols(4, 1)
            add_column_num += 1
            print("當年度資料更新")
            self.msg_queue.put("當年度資料更新")

        if add_column_num <= 0:
            print("No data need to update.")
            self.msg_queue.put("No data need to update.")
        else:
            '''        根據相差月份取相對應數量的資料        '''
            get_data_num = add_column_num * 4
            target_cols = [
                "投資活動之淨現金流入（流出）",
                "營業活動之淨現金流入（流出）",
                "籌資活動之淨現金流入（流出）",
                "期初現金及約當現金餘額",
                "期末現金及約當現金餘額"
            ]
            mapper, dfs = self.get_bundle_data(target_cols, get_data_num, stock_id)
            df = pd.concat(dfs, axis=1).multiply(0.00001)  # 單位:億
            df = df.apply(self.get_cash_flow)
            df["free_cash_flow"] = df["投資活動之淨現金流入（流出）"] + df["營業活動之淨現金流入（流出）"]
            target_cols.append("free_cash_flow")

            target_pos = [
                (4, 4, "理財活動現金"),
                (3, 4, "營業活動現金"),
                (6, 4, "籌資活動現金"),
                (7, 4, "期初現金及約當現金餘額"),
                (8, 4, "期末現金及約當現金餘額"),
                (5, 4, "自由現金流量"),
            ]
            add_column_num *= -1
            for add_row in range(add_column_num, 0, 1):
                self.ws2.insert_cols(4, amount=1)
                update_year = df.index[add_row]
                update_year_str = update_year.strftime('%Y')

                '''  新增年度標籤  '''
                self.write_to_excel(update_year_str, sheet=self.ws2, rows=1, cols=4, string="現金流量標籤", date=update_year_str)
                self.ws2.cell(row=1, column=4).fill = PatternFill(fill_type="solid", fgColor="DDDDDD")

                '''  新增營業活動現金、理財活動現金、自由現金流量、籌資活動現金 新增期初現金及約當現金餘額、期末現金及約當現金餘額'''
                for col, pos in zip(target_cols, target_pos):
                    value = df.loc[update_year, col]
                    self.write_to_excel(value, round_num=1, sheet=self.ws2, rows=pos[0], cols=pos[1], string=pos[2], date=update_year_str)

            try:
                '''   判斷條件   '''
                for c in range(4, 9):
                    # 營業活動現金
                    condition_ocf = int(self.ws2.cell(row=3, column=c).value) < 0
                    self.warning_func(condition_ocf, sheet=self.ws2, rows=3, cols=c, threat='True')
                    # 自由現金
                    condition_fcf = int(self.ws2.cell(row=5, column=c).value) < 0
                    self.warning_func(condition_fcf, sheet=self.ws2, rows=5, cols=c, threat='True')
            except:
                print(f"{stock_id} 警告上色錯誤")
                self.msg_queue.put(f"{stock_id} 警告上色錯誤")

            self.wb.save(path or self.file_path)
            print("完成更新 {} 的 現金流量表".format(stock_id))
            self.msg_queue.put("完成更新 {} 的 現金流量表".format(stock_id))

    async def _update_per(self, stock_id, path=None):
        '''    從資料庫獲取季報最新日期    '''
        # *未結束年度之EPS預估值, 以最近四季之合計EPS取代之, 例如: 某股票EPS僅公布至今年第三季, 則
        # 今年之預估EPS = 去年第四季至今年第三季之合計EPS。
        # https://goodinfo.tw/StockInfo/ShowK_ChartFlow.asp?RPT_CAT=PER&STOCK_ID=2330&CHT_CAT=QUAR

        '''    使用現在的時間當作最新的更新時間點    '''
        now = datetime.datetime.now()
        season_now = self.season_transform(now)

        # 與table最新資料比對時間，決定需要增加的數據量
        table_month = self.ws4["A16"].value
        add_row_num = 4 * (int(season_now[0:4]) - int(table_month[0:4])) + (
                int(season_now[-1]) - int(table_month[-1]))

        if add_row_num <= 0:
            print("Update PER this year.")
            self.msg_queue.put("Update PER this year.")
        else:
            print("Increase PER this season and update PER this year.")
            self.msg_queue.put("Increase PER this season and update PER this year.")

        # 決定要更新多少當年度的PER，抓取excel同年度資料，寫進Update_row
        per_data = [self.ws4.cell(row=n, column=1).value[0:4] for n in range(16, 20) if
                    self.ws4.cell(row=n, column=1).value]
        update_row = 0
        for n in per_data:
            if n == now.strftime("%Y"):
                update_row += 1

        # 根據需要跟新以及新增的數量，去從sqlite3抓取相對應的數據量
        total_num = update_row + add_row_num
        get_data_num = total_num + 4
        equity = self.get_data_assign_table("股本合計", get_data_num) * 0.00001  # 單位: 億
        profit_after_tax = self.get_data_assign_table("本期淨利（淨損）", get_data_num) * 0.00001  # 單位: 億

        price_num = (total_num) * 100
        price = self.get_data_assign_table("收盤價", price_num)

        equity = equity[stock_id].dropna()
        profit_after_tax = profit_after_tax[stock_id].dropna()
        price = price[stock_id].dropna()

        price_q1 = price[price.index.month.isin([1, 2, 3])].sort_index()
        price_q2 = price[price.index.month.isin([4, 5, 6])].sort_index()
        price_q3 = price[price.index.month.isin([7, 8, 9])].sort_index()
        price_q4 = price[price.index.month.isin([10, 11, 12])].sort_index()

        eps = profit_after_tax / (equity / 10)
        estimated_eps = eps.rolling(4).sum()

        '''  檢查公布財報的EPS時間與實際時間的差別，如果尚未公布財報則填入現在的時間，新增最新時間資料  '''
        fr_date = self.season_transform(estimated_eps.index[-1])
        num = 4 * (int(season_now[0:4]) - int(fr_date[0:4])) + (int(season_now[-1]) - int(fr_date[-1]))

        for n in range(num):
            date = self.delta_seasons(estimated_eps.index[-1], -1)
            estimated_eps[date] = estimated_eps[-1]

        estimated_eps.index = self.season_transform(estimated_eps.index, spec=True)

        startrt = 16
        end = 16 + update_row

        # 更新今年度的PER
        for add_row in range(startrt, end):

            # 從財報上資料判斷要更新的季節
            update_season = str(self.ws4.cell(row=add_row, column=1).value)
            if update_season[-1] == "1":
                price = price_q1.loc[update_season[0:4]][-1]
            elif update_season[-1] == "2":
                price = price_q2.loc[update_season[0:4]][-1]
            elif update_season[-1] == "3":
                price = price_q3.loc[update_season[0:4]][-1]
            else:
                price = price_q4.loc[update_season[0:4]][-1]
            e_eps = estimated_eps.loc[update_season]
            per = price / e_eps

            print(f"更新 {self.ws4.cell(row=add_row, column=1).value} 的EPS: {round(e_eps, 2)}")
            self.msg_queue.put(f"更新 {self.ws4.cell(row=add_row, column=1).value} 的EPS: {round(e_eps, 2)}")
            self.write_to_excel(per, round_num=2, sheet=self.ws4, rows=add_row, cols=2, string="更新PER",
                                date=update_season)

        # 新增PER資料
        add_row_num *= -1

        for add_row in range(add_row_num, 0, 1):

            self.ws4.insert_rows(16, amount=1)

            update_season_date = estimated_eps.index[add_row]

            '''  新增季度標籤  '''
            update_season = self.season_transform(update_season_date)

            self.ws4.cell(row=16, column=1).value = update_season
            self.ws4.cell(row=16, column=1).alignment = Alignment(horizontal="center", vertical="center",
                                                                  wrap_text=True)
            self.ws4.cell(row=16, column=1).fill = PatternFill(fill_type="solid", fgColor="FFEE99")
            print(f"新增標籤: {self.ws4.cell(row=16, column=1).value}")
            self.msg_queue.put(f"新增標籤: {self.ws4.cell(row=16, column=1).value}")

            '''  新增本益比  '''
            if update_season:
                if update_season[-1] == "1":
                    price = price_q1.loc[update_season[0:4]][-1]
                elif update_season[-1] == "2":
                    price = price_q2.loc[update_season[0:4]][-1]
                elif update_season[-1] == "3":
                    price = price_q3.loc[update_season[0:4]][-1]
                else:
                    price = price_q4.loc[update_season[0:4]][-1]
            else:
                price = 0.0
            e_eps = estimated_eps.loc[update_season]
            per = price / e_eps

            print(f"使用季度: {update_season} 所得到的EPS: {round(e_eps, 2)}")
            self.msg_queue.put(f"使用季度: {update_season} 所得到的EPS: {round(e_eps, 2)}")
            self.write_to_excel(per, round_num=2, sheet=self.ws4, rows=16, cols=2, string="新增PER", date=update_season)

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 本益比".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 本益比".format(stock_id))

    async def update_per(self, stock_id, path=None):
        '''    使用現在的時間當作最新的更新時間點    '''
        now = datetime.datetime.now()
        season_now = self.season_transform(now)

        # 與table最新資料比對時間，決定需要增加的數據量
        table_month = self.ws4["A16"].value
        diff_year, diff_season = (int(season_now[0:4]) - int(table_month[0:4])), (int(season_now[-1]) - int(table_month[-1]))
        add_row_num = 4 * diff_year + diff_season

        if add_row_num <= 0:
            print("Update PER this year.")
            self.msg_queue.put("Update PER this year.")
        else:
            print("Increase PER this season and update PER this year.")
            self.msg_queue.put("Increase PER this season and update PER this year.")

        # 決定要更新多少當年度的PER，抓取excel同年度資料，寫進Update_row
        update_row_num = 0 if diff_year != 0 else diff_season

        # 根據需要跟新以及新增的數量，去從sqlite3抓取相對應的數據量
        total_num = update_row_num + add_row_num
        get_data_num = total_num + 4
        equity = self.get_data_assign_table("股本合計", get_data_num) * 0.00001  # 單位: 億
        profit_after_tax = self.get_data_assign_table("本期淨利（淨損）", get_data_num) * 0.00001  # 單位: 億

        price_num = total_num * 100
        price_df = self.get_data_assign_table("收盤價", price_num)

        equity = equity[stock_id].dropna()
        profit_after_tax = profit_after_tax[stock_id].dropna()
        price_df = price_df[stock_id].dropna()
        price_df.index = price_df.index.to_period("Q")
        price_df = price_df.groupby([price_df.index]).last()

        eps = profit_after_tax / (equity / 10)
        estimated_eps = eps.rolling(4).sum()

        '''  檢查公布財報的EPS時間與實際時間的差別，如果尚未公布財報則填入現在的時間，新增最新時間資料  '''
        fr_date = self.season_transform(estimated_eps.index[-1])
        num = 4 * (int(season_now[0:4]) - int(fr_date[0:4])) + (int(season_now[-1]) - int(fr_date[-1]))
        for n in range(num):
            date = self.delta_seasons(estimated_eps.index[-1], -1)
            estimated_eps[date] = estimated_eps[-1]

        estimated_eps.index = self.season_transform(estimated_eps.index, spec=True)

        # 新增PER資料
        for add_row in range(-1*total_num, 0, 1):
            row = 16 + -1*(add_row+update_row_num) if add_row + update_row_num < -1*add_row_num else 16
            if row == 16:
                self.ws4.insert_rows(16, amount=1)

            update_season_date = estimated_eps.index[add_row]

            '''  新增季度標籤  '''
            update_season = self.season_transform(update_season_date)
            self.write_to_excel(update_season, sheet=self.ws4, rows=row, cols=1, string="PER季度標籤",
                                date=f"{update_season}")
            self.ws4.cell(row=row, column=1).fill = PatternFill(fill_type="solid", fgColor="DDDDDD")

            '''  新增本益比  '''
            try:
                price = price_df.loc[update_season]
            except Exception as e:
                print(f"有問題發生，請檢查{e}")
                self.msg_queue.put(f"有問題發生，請檢查{e}")
                price = 0.0

            e_eps = estimated_eps.loc[update_season]
            per = price / e_eps

            print(f"使用季度: {update_season} 所得到的EPS: {round(e_eps, 2)}")
            self.msg_queue.put(f"使用季度: {update_season} 所得到的EPS: {round(e_eps, 2)}")
            self.write_to_excel(per, round_num=2, sheet=self.ws4, rows=row, cols=2, string="新增PER", date=update_season)

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 本益比".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 本益比".format(stock_id))

    async def _update_price_today(self, stock_id, path=None):
        highest = self.get_data('最高價', 1)
        lowest = self.get_data('最低價', 1)
        opening = self.get_data('開盤價', 1)
        closing = self.get_data('收盤價', 1)

        highest = highest[stock_id]
        lowest = lowest[stock_id]
        opening = opening[stock_id]
        closing = closing[stock_id]

        dates = highest.index[0]

        dates_str = dates.strftime("%Y/%m/%d")

        self.ws4.cell(row=13, column=1).value = dates_str
        self.ws4.cell(row=13, column=1).alignment = Alignment(horizontal="center", vertical="center",
                                                              wrap_text=True)
        self.write_to_excel(highest.iloc[0], round_num=1, sheet=self.ws4, rows=12, cols=3, string="新增最高價",
                            date=dates_str)
        self.write_to_excel(lowest.iloc[0], round_num=1, sheet=self.ws4, rows=13, cols=3, string="新增最低價",
                            date=dates_str)
        self.write_to_excel(opening.iloc[0], round_num=1, sheet=self.ws4, rows=12, cols=5, string="新增開盤價",
                            date=dates_str)
        self.write_to_excel(closing.iloc[0], round_num=1, sheet=self.ws4, rows=13, cols=5, string="新增收盤價",
                            date=dates_str)

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 價位".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 價位".format(stock_id))

    async def update_price_today(self, stock_id, path=None):
        total_cols = ['最高價', '最低價', '開盤價', '收盤價']
        excel_pos = [(12, 3), (13, 3), (12, 5), (13, 5)]

        mapper, dfs = self.get_bundle_data(total_cols, 1, stock_id)
        df = pd.concat(dfs, axis=1)

        date_str = df.index[0].strftime("%Y/%m/%d")

        self.write_to_excel(
            date_str, sheet=self.ws4, rows=13, cols=1, string=f"新增{date_str}"
        )
        for col, pos in zip(total_cols, excel_pos):
            self.write_to_excel(
                df[col].iloc[0], round_num=1, sheet=self.ws4, rows=pos[0], cols=pos[1], string=f"新增{col}", date=date_str
            )

        self.wb.save(path or self.file_path)
        print("完成更新 {} 的 價位".format(stock_id))
        self.msg_queue.put("完成更新 {} 的 價位".format(stock_id))


class SelectStock(RetrieveDataModule):
    def __init__(self, conn, msg_queue):
        super().__init__(conn, msg_queue)
        self.msg_queue = msg_queue

    async def my_strategy(self, date, cond_content, activate):
        share_capital = self.get_data(name='股本合計', n=1, start=date)
        price = self.get_data(name='收盤價', n=120, start=date)
        price_today = price[:share_capital.index[-1]].iloc[-1]
        capital_today = share_capital.iloc[-1]
        market_value = capital_today * price_today / 10 * 1000

        df1 = self.data_process(self.get_data(name='投資活動之淨現金流入（流出）', n=15, start=date))
        df2 = self.data_process(self.get_data(name='營業活動之淨現金流入（流出）', n=15, start=date))
        three_yrs_cash_flow = (df1 + df2).iloc[-12:].mean()

        net_profit_after_tax = self.get_data(name='本期淨利（淨損）', n=9, start=date)
        # 股東權益，有兩個名稱，有些公司叫做權益總計，有些叫做權益總額，所以得把它們抓出來
        total_stockholders_equity = self.get_data(name='權益總計', n=1, start=date)
        total_equity = self.get_data(name='權益總額', n=1, start=date)
        # 並且把它們合併起來
        total_stockholders_equity.fillna(total_equity, inplace=True)

        return_on_equity = ((net_profit_after_tax.iloc[-4:].sum()) / total_stockholders_equity.iloc[-1]) * 100

        operating_profit = self.get_data(name='營業利益（損失）', n=9, start=date)
        revenue_season = self.get_data(name='營業收入合計', n=9, start=date)
        operating_profit_margin = operating_profit / revenue_season
        prev_season_opm = operating_profit.shift(1) / revenue_season.shift(1)
        yr_growth_of_opm = (operating_profit_margin.iloc[-1] / operating_profit_margin.iloc[-5] - 1) * 100
        eight_seasons_opm = (operating_profit_margin / prev_season_opm - 1) * 100
        eight_seasons_opm = eight_seasons_opm.dropna(axis=1, how="all").dropna(how="all")

        revenue_this_month = self.get_data(name='當月營收', n=12, start=date) * 1000
        revenue_this_year = revenue_this_month.iloc[-12:].sum()
        market_value_per_revenue = market_value / revenue_this_year

        mr_year_growth = self.get_data(name='去年同月增減(%)', n=12, start=date)
        rolling_3_months_mr_year_growth = mr_year_growth.rolling(3).mean().reindex(index=mr_year_growth.index).iloc[-1]
        rolling_12_months_mr_year_growth = mr_year_growth.rolling(12).mean().reindex(index=mr_year_growth.index).iloc[-1]

        net_profit_margin_after_tax = net_profit_after_tax / revenue_season
        last_yr_net_profit_margin_after_tax = net_profit_margin_after_tax.shift(4)
        yr_growth_npm = (net_profit_margin_after_tax - last_yr_net_profit_margin_after_tax) / last_yr_net_profit_margin_after_tax * 100
        yr_growth_npm = yr_growth_npm
        short_term_yr_growth_npm = yr_growth_npm.iloc[-1]
        long_term_yr_growth_npm = yr_growth_npm[-4:].mean()

        inventory = self.get_data(name="存貨", n=3, start=date)
        operating_cost = self.get_data(name="營業成本合計", n=2, start=date)
        inventory_turnover = operating_cost.iloc[-1] / ((inventory.iloc[-1] + inventory.iloc[-2]) / 2) * 4
        prev_season_inventory_turnover = operating_cost.iloc[-2] / ((inventory.iloc[-2] + inventory.iloc[-3]) / 2) * 4
        inventory_turnover_ratio = (inventory_turnover - prev_season_inventory_turnover) / prev_season_inventory_turnover * 100

        rsv = (price.iloc[-1] - price.iloc[-60:].min()) / (price.iloc[-60:].max() - price.iloc[-60:].min())

        mapper = {
            "市值": market_value,
            "三年自由現金流": three_yrs_cash_flow,
            "股東權益報酬率": return_on_equity,
            "營業利益年成長率": yr_growth_of_opm,
            "八季營益率變化": eight_seasons_opm,
            "市值營收比": market_value_per_revenue,
            "短期營收年增": rolling_3_months_mr_year_growth,
            "短期營收年增2": rolling_3_months_mr_year_growth,
            "長期營收年增": rolling_12_months_mr_year_growth,
            "短期淨利年增": short_term_yr_growth_npm,
            "長期淨利年增": long_term_yr_growth_npm,
            "存貨周轉變化率": inventory_turnover_ratio,
            "rsv": rsv
        }
        ops = {
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
            "=": operator.eq,
        }
        def _operator_func(var, op, con):
            a = mapper[var]
            if con in mapper:
                value = mapper[con]
            else:
                value = float(con)
            return ops[op](a, value)

        condition_list = []
        for b, e in zip(activate, cond_content):
            if len(e.split()) >= 3 and b is True:
                operators = _operator_func(*(e.split()))
                if isinstance(operators, pd.DataFrame):
                    operators = mapper[e.split()[0]][operators].isnull().sum() <= 0
                condition_list.append(operators)
        select_stock = condition_list[0]
        for cond in condition_list:
            select_stock = select_stock & cond
        return select_stock[select_stock]

    async def backtest(self, start_date, end_date, hold_days, cond_content, activate, weight='average', benchmark=None,
                 stop_loss=None,
                 stop_profit=None):
        # portfolio check
        if weight != 'average' and weight != 'price':
            print(f'Backtest stop, weight should be "average" or "price", find {weight} instead')
            self.msg_queue.put(f'Backtest stop, weight should be "average" or "price", find {weight} instead')

        # get price data in order backtest
        self.date = end_date
        price = self.get_data('收盤價', (end_date - start_date).days)
        # start from 1 TWD at start_date,
        end = 1

        # record some history
        equality = pd.Series()
        n_stock = {}
        comparison = []
        transactions = pd.DataFrame()
        max_return = -10000
        min_return = 10000

        if isinstance(hold_days, int):
            dates = self._date_iter_periodicity(start_date, end_date, hold_days)
        elif isinstance(hold_days, list):
            dates = self._date_iter_specify_dates(start_date, end_date, hold_days)
        else:
            print('the type of hold_dates should be list or int.')
            self.msg_queue.put('the type of hold_dates should be list or int.')
            return None

        figure, ax = plt.subplots(2, 1, sharex=True, sharey=False)

        keep_list = []
        keep_idx = pd.Index(keep_list)
        for sdate, edate in dates:
            # select stocks at date
            self.date = sdate
            stocks = await self.my_strategy(sdate, cond_content, activate)

            idx = stocks.index.append([keep_idx]).drop_duplicates()
            print(f"回測的股票為: {idx.tolist()}")
            self.msg_queue.put(f"回測的股票為: {idx.tolist()}")
            selected_columns = price[idx.tolist()]

            result = selected_columns[sdate:edate].iloc[1:]
            s = price[idx][sdate:edate].iloc[1:]

            if s.empty:
                s = pd.Series(1, index=pd.date_range(sdate + datetime.timedelta(days=1), edate))
            else:
                if stop_loss is not None:
                    below_stop = ((s / s.bfill().iloc[0]) - 1) * 100 < -np.abs(stop_loss)
                    below_stop = (below_stop.cumsum() > 0).shift(2).fillna(False)
                    s[below_stop] = np.nan
                if stop_profit is not None:
                    above_stop = ((s / s.bfill().iloc[0]) - 1) * 100 > np.abs(stop_profit)
                    above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                    s[above_stop] = np.nan

                s.dropna(axis=1, how='all', inplace=True)
                keep_list = s.dropna(axis=1)
                keep_idx = pd.Index(keep_list.columns)

                # record transactions
                buy_price = s.bfill().iloc[0]
                sell_price = s.apply(lambda s: s.dropna().iloc[-1])
                append_tran = pd.DataFrame({
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'lowest_price': s.min(),
                    'highest_price': s.max(),
                    'buy_date': pd.Series(s.index[0], index=s.columns),
                    'sell_date': s.apply(lambda s: s.dropna().index[-1]),
                    'profit(%)': (sell_price / buy_price - 1) * 100
                })
                transactions = pd.concat([transactions, append_tran]).sort_index(ascending=True)

                s.ffill(inplace=True)
                s = s.sum(axis=1)

                if weight == 'average':
                    s = s / s.bfill().iloc[0]
                else:
                    s = s / s.bfill()[0]

            # print some log
            start_time = sdate.strftime("%Y-%m-%d")
            end_time = edate.strftime("%Y-%m-%d")

            profit_str = "{} - {} 報酬率: {:.2f}% nstock {}".format(start_time, end_time,
                                                                    (s.iloc[-1] / s.iloc[0] * 100 - 100), len(idx))
            comparison.append(profit_str)
            benchmark1 = price['0050'][sdate:edate].iloc[1:]
            p0050_str = "{} - {} 的0050報酬率: {:.2f}% ".format(start_time, end_time,
                                                                (benchmark1.iloc[-1] / benchmark1.iloc[0] * 100 - 100))
            comparison.append(p0050_str)
            print(f"{profit_str}\n{p0050_str}")
            self.msg_queue.put(f"{profit_str}\n{p0050_str}")

            max_return = max(max_return, s.iloc[-1] / s.iloc[0] * 100 - 100)
            min_return = min(min_return, s.iloc[-1] / s.iloc[0] * 100 - 100)

            # plot backtest result
            ((s * end - 1) * 100).plot(ax=ax[0])
            equality = pd.concat([equality, s * end])
            end = (s / s[0] * end).iloc[-1]

            if math.isnan(end):
                end = 1

            # add nstock history
            n_stock[sdate] = len(stocks)

            print('每次換手最大報酬 : %.2f ％' % max_return)
            print('每次換手最少報酬 : %.2f ％' % min_return)
            self.msg_queue.put('每次換手最大報酬 : %.2f ％' % max_return)
            self.msg_queue.put('每次換手最少報酬 : %.2f ％' % min_return)

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

        return equality, transactions, max_return, min_return, comparison

    @staticmethod
    def _date_iter_periodicity(start_date, end_date, hold_days):
        date = start_date
        while date < end_date:
            yield date, (date + datetime.timedelta(hold_days))
            date += datetime.timedelta(hold_days)

    @staticmethod
    def _date_iter_specify_dates(start_date, end_date, hold_days):
        date_list = [start_date] + hold_days + [end_date]
        if date_list[0] == date_list[1]:
            date_list = date_list[1:]
        if date_list[-1] == date_list[-2]:
            date_list = date_list[:-1]
        for s_date, e_date in zip(date_list, date_list[1:]):
            yield s_date, e_date


class TWStockRetrieveModule(RetrieveDataModule):
    def __init__(self, conn):
        super().__init__(conn)

        self.price = self.get_data('收盤價', 800)
        self.revenue_month = self.get_data('當月營收', 36)
        self.mr_month_growth = self.get_data('上月比較增減(%)', 36)
        self.mr_year_growth = self.get_data('去年同月增減(%)', 48)

        self.revenue_season = self.get_data_assign_table('營業收入合計', 16) * 0.00001  # 單位: 億
        # 營業利益率，也可以簡稱營益率，英文Operating Margin或Operating profit Margin
        self.opm_raw = self.get_data_assign_table('營業利益（損失）', 16) * 0.00001  # 單位: 億
        self.gross_profit = self.get_data_assign_table('營業毛利（毛損）', 16) * 0.00001  # 單位: 億
        self.equity = self.get_data_assign_table("股本合計", 16) * 0.00001  # 單位: 億
        self.profit_before_tax = self.get_data_assign_table("繼續營業單位稅前淨利（淨損）",
                                                            16) * 0.00001  # 單位: 億  本期稅前淨利（淨損）
        self.profit_after_tax = self.get_data_assign_table("本期淨利（淨損）", 16) * 0.00001  # 單位: 億
        self.operating_costs = self.get_data_assign_table("營業成本合計", 16) * 0.00001  # 單位: 億
        self.account_receivable = self.get_data_assign_table("應收帳款淨額", 16) * 0.00001  # 單位: 億
        self.inventory = self.get_data_assign_table("存貨", 16) * 0.00001  # 單位: 億
        self.assets = self.get_data_assign_table("資產總計", 16) * 0.00001  # 單位: 億
        self.liabilities = self.get_data_assign_table("負債總計", 16) * 0.00001  # 單位: 億
        self.accounts_payable = self.get_data_assign_table("應付帳款", 16) * 0.00001  # 單位: 億
        self.intangible_assets = self.get_data_assign_table("無形資產", 16) * 0.00001  # 單位: 億
        self.depreciation = self.get_data_assign_table("折舊費用", 16, table="cash_flows") * 0.00001  # 單位: 億
        self.net_income = self.get_data_assign_table('本期淨利（淨損）', 16) * 0.00001  # 單位: 億
        # 修正：因為有些股東權益的名稱叫作「權益總計」有些叫作「權益總額」，所以要先將這兩個dataframe合併起來喔！
        權益總計 = self.get_data_assign_table('權益總計', 16)
        權益總額 = self.get_data_assign_table('權益總額', 16)
        # 把它們合併起來（將「權益總計」為NaN的部分填上「權益總額」）
        self.Shareholders_equity = 權益總計.fillna(權益總額, inplace=False) * 0.00001  # 單位: 億

        # Cash Flow for investing
        self.cash_flow_for_investing = self.get_data_assign_table("投資活動之淨現金流入（流出）", 32) * 0.00001  # 單位:億
        # Operating Cash Flow
        self.operating_cash_flow = self.get_data_assign_table("營業活動之淨現金流入（流出）", 32) * 0.00001  # 單位:億
        # Cash Flows Provided from Financing Activities
        self.cash_flow_for_financing = self.get_data_assign_table("籌資活動之淨現金流入（流出）", 32) * 0.00001  # 單位:億
        # Cash Balances - Beginning of Period
        self.cash_balances_beginning = self.get_data_assign_table("期初現金及約當現金餘額", 32) * 0.00001  # 單位:億
        # Cash Balances - End of Period
        self.cash_balances_end = self.get_data_assign_table("期末現金及約當現金餘額", 32) * 0.00001  # 單位:億

        self.mapper = {
            "股價": self.price,
            "月營收": self.revenue_month,
            "月營收月增率": self.mr_month_growth,
            "月營收年增率": self.mr_year_growth,
            "季營收": self.revenue_season,
            "營業利益": self.opm_raw,
            "營業毛利": self.gross_profit,
            "股本": self.equity,
            "稅前淨利": self.profit_before_tax,
            "稅後淨利": self.profit_after_tax,
            "營業成本": self.operating_costs,
            "應收帳款": self.account_receivable,
            "存貨": self.inventory,
            "總資產": self.assets,
            "總負債": self.liabilities,
            "應付帳款": self.accounts_payable,
            "無形資產": self.intangible_assets,
            "折舊": self.depreciation,
            "本期淨利": self.net_income,
            "股東權益": self.Shareholders_equity,
        }

    def retrieve_month_data(self, stock_id):
        # 輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票
        month_revenue = self.revenue_month[stock_id].rename("月營收(百萬)")
        month_revenue = month_revenue.astype(int).apply(lambda s: round(s / 1000000, 2))
        price = self.price[stock_id].rename("股價")
        mr_month_growth = self.mr_month_growth[stock_id]
        mr_year_growth = self.mr_year_growth[stock_id].rename("月營收年增")

        month_revenue.index = pd.to_datetime(month_revenue.index.strftime("%Y-%m"), format="%Y-%m")
        price.index = pd.to_datetime(price.index.strftime("%Y-%m"), format="%Y-%m")
        mr_month_growth.index = pd.to_datetime(mr_month_growth.index.strftime("%Y-%m"), format="%Y-%m")
        mr_year_growth.index = pd.to_datetime(mr_year_growth.index.strftime("%Y-%m"), format="%Y-%m")

        price.index = pd.to_datetime(price.index.strftime("%Y-%m"), format="%Y-%m")
        price_df = price.groupby(price.index).aggregate(['min', 'mean', 'max'])
        price_df = price_df.rename(columns={
            "min": "最低股價",
            "mean": "平均股價",
            "max": "最高股價"
        })

        mag_3_m = mr_year_growth.rolling(3).mean().reindex(index=mr_year_growth.index).rename("營收年增3個月移動平均")
        mag_3_m = round(mag_3_m, 2)
        mag_12_m = mr_year_growth.rolling(12).mean().reindex(index=mr_year_growth.index).rename(
            "營收年增12個月移動平均")
        mag_12_m = round(mag_12_m, 2)

        dfs = [month_revenue, mr_year_growth, price_df, mag_3_m, mag_12_m]
        final = pd.concat(dfs, join="inner", axis=1).sort_index(ascending=False)
        final.index = final.index.strftime('%b-%y')

        return final.reset_index().rename(columns={"index": "月份"})

    def retrieve_season_data(self, stock_id):
        '''        輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票        '''
        revenue_season = self.revenue_season[stock_id]
        opm_raw = self.opm_raw[stock_id]
        gross_profit = self.gross_profit[stock_id]
        equity = self.equity[stock_id]
        price = self.price[stock_id]
        profit_before_tax = self.profit_before_tax[stock_id]
        profit_after_tax = self.profit_after_tax[stock_id]
        operating_costs = self.operating_costs[stock_id]
        account_receivable = self.account_receivable[stock_id]
        inventory = self.inventory[stock_id]
        assets = self.assets[stock_id]
        liabilities = self.liabilities[stock_id]
        accounts_payable = self.accounts_payable[stock_id]
        intangible_assets = self.intangible_assets[stock_id]
        depreciation = self.depreciation[stock_id]
        net_income = self.net_income[stock_id]
        shareholders_equity = self.Shareholders_equity[stock_id]

        '''        拆解數據處理        '''
        decom_depreciation = self.data_process(depreciation, cum=False)
        '''        累積數據處理        '''
        cum_return_on_equity = net_income / shareholders_equity * 100
        cum_return_on_equity = self.data_process(cum_return_on_equity, cum=True)

        cum_profit_after_tax = self.data_process(profit_after_tax, cum=True)
        cum_revenue_season = self.data_process(revenue_season, cum=True)
        cum_profit_after_tax = cum_profit_after_tax / cum_revenue_season * 100

        cum_shareholders_equity = shareholders_equity / assets * 100

        new_assets = []
        for idx in range(len(assets)):
            new_assets.append((assets[idx] + assets[idx - 1]) / 2)
        new_assets = pd.Series(new_assets, index=assets.index)
        new_assets = new_assets.drop(labels=[assets.index[0]])
        cum_new_assets = self.data_process(new_assets, cum=True)
        cum_total_assets_turnover = cum_revenue_season / cum_new_assets * 4

        '''  新增當期營收、當期營收年成長率  '''
        sr = revenue_season
        sr_yg = (sr - sr.shift(4)) / sr.shift(4) * 100
        sr, sr_yg = round(sr, 1).rename("季營收(億)"), round(sr_yg, 1).rename("季營收年增")
        '''   營業毛利率   '''
        gp = gross_profit / revenue_season * 100
        gp = round(gp, 1).rename("營業毛利率")
        '''   營業利益率、營業利益成長率   '''
        opm = opm_raw / revenue_season * 100
        opm_sg = (opm - opm.shift(1)) / opm.shift(1) * 100
        opm, opm_sg = round(opm, 1).rename("營業利益率"), round(opm_sg, 1).rename("營業利益成長率")
        '''   新增股本、股本季增率、當期市值與市值營收比   '''
        eq = equity
        eq_sg = (eq - equity.shift(1)) / eq.shift(1) * 100
        price_eq = price.loc[:eq.index[-1]].iloc[-1]  # 確認股本公布當天是否為交易日
        mv = price_eq * eq / 10  # 市值 = 股價 * 總股數 (股本合計單位為 k元)
        psr = revenue_season.rolling(4).sum() / mv * 100
        eq, eq_sg = round(eq, 1).rename("股本"), round(eq_sg, 1).rename("股本季增率")
        mv, psr = round(mv, 1).rename("當期市值"), round(psr, 1).rename("市值營收比")
        '''   新增稅前淨利率、本業收入比率、稅後淨利率、稅後淨利年增率  '''
        pbt = profit_before_tax / revenue_season * 100
        so_r = opm / pbt
        pat = profit_after_tax / revenue_season * 100
        pat_yg = (profit_after_tax - profit_after_tax.shift(4)) / profit_after_tax.shift(4) * 100
        pbt, so_r = round(pbt, 1).rename("稅前淨利率"), round(so_r, 1).rename("本業收入比率")
        pat, pat_yg = round(pat, 1).rename("稅後淨利率"), round(pat_yg, 1).rename("稅後淨利年增率")
        '''   新增EPS、EPS年成長率   '''
        eps = profit_after_tax / (equity / 10)
        eps_yg = (eps - eps.shift(4)) / eps.shift(4) * 100
        eps, eps_yg = round(eps, 1).rename("EPS"), round(eps_yg, 1).rename("EPS年成長率")
        '''   新增應收帳款週轉率、存貨周轉率、存貨營收比   '''
        # receivables turnover
        rt = sr / ((account_receivable + account_receivable.shift(1)) / 2) * 4
        # inventory turnover
        it = operating_costs / ((inventory + inventory.shift(1)) / 2) * 4
        # inventory revenue ratio
        ir = inventory / sr * 100
        rt, it = round(rt, 1).rename("應收帳款週轉率"), round(it, 1).rename("存貨周轉率")
        ir = round(ir, 1).rename("存貨營收比")
        '''   新增應付帳款總資產占比、負債總資產占比、無形資產占比'''
        li_a = liabilities / assets * 100
        ap = accounts_payable / assets * 100
        int_a = intangible_assets / assets * 100
        li_a, ap = round(li_a, 1).rename("應付帳款總資產占比"), round(ap, 1).rename("負債總資產占比")
        int_a = round(int_a, 1).rename("無形資產占比")
        '''   新增折舊、折舊負擔比率   '''
        # Debt Asset ratio
        dep = decom_depreciation
        dar = dep / sr
        dep, dar = round(dep, 1).rename("折舊"), round(dar, 1).rename("折舊負擔比率")
        '''   杜邦分析   '''
        c_roe = cum_return_on_equity
        ce_roe = c_roe
        ce_roe.update(cum_return_on_equity[cum_return_on_equity.index.month == 5] * 4)
        ce_roe.update(cum_return_on_equity[cum_return_on_equity.index.month == 8] * 2)
        ce_roe.update(cum_return_on_equity[cum_return_on_equity.index.month == 11] * 4 / 3)
        c_tat = cum_total_assets_turnover
        c_pat = cum_profit_after_tax
        c_se = cum_shareholders_equity
        # Equity Multiplier
        c_em = 1 / c_se * 100
        c_roe, ce_roe = round(c_roe, 1).rename("股東權益報酬率(季)"), round(ce_roe, 1).rename("股東權益報酬率(年預估)")
        c_pat, c_tat = round(c_pat, 1).rename("稅後淨利率(累計)"), round(c_tat, 1).rename("總資產週轉率(次/年)")
        c_em, c_se = round(c_em, 1).rename("權益係數"), round(c_se, 1).rename("股東權益總額(%)")

        empty_profit = pd.Series(name='* 獲利能力', index=sr.index)
        empty_operation = pd.Series(name='* 經營能力', index=sr.index)
        empty_asset = pd.Series(name='* 資產負債表', index=sr.index)
        empty_cash = pd.Series(name='* 現金流量表', index=sr.index)
        empty_du_pont = pd.Series(name='* 杜邦分析(累季)', index=sr.index)

        dfs = [
            empty_profit, sr, sr_yg, mv, gp, opm, opm_sg, pbt, so_r, pat, pat_yg, eps, eps_yg,
            empty_operation, rt, it, ir, psr,
            empty_asset, eq, eq_sg, li_a, ap, int_a,
            empty_cash, dep, dar,
            empty_du_pont, c_roe, ce_roe, c_pat, c_tat, c_em, c_se
        ]
        final = pd.concat(dfs, join="inner", axis=1).sort_index(ascending=False)
        final.index = final.index.to_period("Q").strftime('%yQ%q')
        final = final.transpose()

        return final.reset_index().rename(columns={"index": "項目"})

    def retrieve_cash_data(self, stock_id):
        '''        輸入數字並存在變數中，可以透過該變數(字串)，呼叫特定股票        '''
        cash_flow_for_investing = self.cash_flow_for_investing[stock_id]
        operating_cash_flow = self.operating_cash_flow[stock_id]
        # Free cash flow(FCF)
        free_cash_flow = (cash_flow_for_investing + operating_cash_flow)
        cash_flow_for_financing = self.cash_flow_for_financing[stock_id]
        cash_balances_beginning = self.cash_balances_beginning[stock_id]
        cash_balances_end = self.cash_balances_end[stock_id]

        cash_flow_for_investing = self.get_cash_flow(cash_flow_for_investing)
        operating_cash_flow = self.get_cash_flow(operating_cash_flow)
        free_cash_flow = self.get_cash_flow(free_cash_flow)
        cash_flow_for_financing = self.get_cash_flow(cash_flow_for_financing)
        cash_balances_beginning = self.get_cash_flow(cash_balances_beginning)
        cash_balances_end = self.get_cash_flow(cash_balances_end)

        '''  新增營業活動現金、理財活動現金、自由現金流量、籌資活動現金'''
        ocf = round(operating_cash_flow, 1).rename("營業活動現金")
        icf = round(cash_flow_for_investing, 1).rename("理財活動現金")
        fcf = round(free_cash_flow, 1).rename("自由現金流量")
        cfpfa = round(cash_flow_for_financing, 1).rename("籌資活動現金")
        cbbp = round(cash_balances_beginning, 1).rename("期初現金及約當現金餘額")
        cbep = round(cash_balances_end, 1).rename("期末現金及約當現金餘額")

        dfs = [ocf, icf, fcf, cfpfa, cbbp, cbep]
        final = pd.concat(dfs, join="inner", axis=1).sort_index(ascending=False)
        final.index = final.index.strftime('%Y')
        final = final.T

        return final.reset_index().rename(columns={"index": "年度"})

    def price_estimation(self, stock_id):
        revenue_month = self.revenue_month[stock_id].rename("月營收(百萬)")
        mr_year_growth = self.mr_year_growth[stock_id].rename("月營收年增")
        revenue_season = self.revenue_season[stock_id]
        profit_after_tax = self.profit_after_tax[stock_id]
        pat_for_per = self.profit_after_tax[stock_id].dropna()
        equity = self.equity[stock_id].dropna()
        price = self.price[stock_id].dropna()
        pat = profit_after_tax / revenue_season * 100

        price.index = price.index.to_period("Q")
        price = price.groupby(price.index).last()
        eps = (profit_after_tax / (equity / 10)).rolling(4).sum()
        eps.index = eps.index.to_period("Q")
        per = price / eps
        per = per.append(per.aggregate(['min', 'mean', 'max']))

        df = pd.DataFrame(dtype=float)
        df["短期"] = [revenue_month.iloc[:3].sum() * 0.000001, mr_year_growth.iloc[:3].sum(), pat.iloc[:4].mean()]
        df["中期"] = [revenue_month.iloc[:6].sum() * 0.000001, mr_year_growth.iloc[:6].sum(), pat.iloc[:8].mean()]
        df["長期"] = [revenue_month.iloc[:12].sum() * 0.000001, mr_year_growth.iloc[:12].sum(), pat.iloc[:12].mean()]

        df = df.rename(index={0: "月營收", 1: "營收年增", 2: "平均稅後淨利"})

        max_mr_yg, min_mr_yg = df.loc["營收年增"].max(), df.loc["營收年增"].min()
        pat_1st = df["短期"].loc["平均稅後淨利"]

        grest_pat = pd.DataFrame({
            "短期": [revenue_month.iloc[-3:].sum() * 0.000001 * (1 + max_mr_yg / 100)],
            "中期": [revenue_month.iloc[-6:].sum() * 0.000001 * (1 + max_mr_yg / 100)],
            "長期": [revenue_month.iloc[-12:].sum() * 0.000001 * (1 + min_mr_yg / 100)],
        }).T
        brest_pat = pd.DataFrame({
            "短期": [revenue_month.iloc[-3:].sum() * 0.000001 * (1 + min_mr_yg / 100)],
            "中期": [revenue_month.iloc[-6:].sum() * 0.000001 * (1 + min_mr_yg / 100)],
            "長期": [revenue_month.iloc[-12:].sum() * 0.000001 * (1 + min_mr_yg / 100)],
        }).T

        est_df = pd.DataFrame({
            "樂觀推估營收": df.loc["月營收"] * (1 + max_mr_yg / 100),
            "悲觀推估營收": df.loc["月營收"] * (1 + min_mr_yg / 100),
        })
        est_df["樂觀推估稅後淨利"] = est_df["樂觀推估營收"] * pat_1st
        est_df["悲觀推估稅後淨利"] = est_df["悲觀推估營收"] * pat_1st
        est_df["樂觀推估eps"] = (est_df["樂觀推估稅後淨利"] + grest_pat[0]) / equity.iloc[-1]
        est_df["悲觀推估eps"] = (est_df["悲觀推估稅後淨利"] + brest_pat[0]) / equity.iloc[-1]
        est_df["樂觀推估價位"] = est_df["樂觀推估eps"] * per.loc["mean"]
        est_df["悲觀推估稅後淨利"] = est_df["悲觀推估eps"] * per.loc["mean"]

        # Empty_profit = pd.Series(table_name='* 獲利能力', index=SR.index)

        dfs = [est_df, per]
        final = pd.concat(dfs).rename(columns={0: "本益比"}).round(1).T

        return final.reset_index().rename(columns={"index": "項目"})

    def module_data_to_draw(self, stock_id, setting):
        df_list = []
        for m in setting.get_data("main"):
            df = self.dict.get_data(m)
            df = df[stock_id].rename(f"m*{m}")
            df.index = pd.to_datetime(df.index.strftime("%Y-%m"), format="%Y-%m")
            if m == "股價":
                df = df.groupby(df.index).mean().sort_values()
            df = round(df, 2)
            df_list.append(df)

        for s in setting.get_data("sub", []):
            ma = re.match(r"([\u4e00-\u9fa5]+)(\d+)\w+移動平均", s)
            if ma:
                s1 = ma.group(1)
                month = int(ma.group(2))
            else:
                s1 = s
                month = None
            df = self.mapper.get_data(s1)
            df = df[stock_id].rename(f"s*{s}")
            df.index = pd.to_datetime(df.index.strftime("%Y-%m"), format="%Y-%m")
            if month:
                df = df.rolling(month).mean().reindex(index=df.index).rename(f"s*{s}")
            df = round(df, 2)
            df_list.append(df)

        final = pd.concat(df_list, join="inner", axis=1).sort_index(ascending=False)
        final.index = final.index.strftime('%b-%y').rename("日期")

        return final, setting

    @staticmethod
    def get_cash_flow(raw_data):
        raw_data = raw_data.fillna(0)
        idx = raw_data.index[-1]
        # 抓當年度最新一筆資料
        raw_data_1 = pd.Series(raw_data[-1], index=[idx])
        # Q4
        if idx.month == 3:
            raw_data_year = idx.year - 1
        else:
            raw_data_year = idx.year

        # 抓每年的Q4
        new_data = raw_data[raw_data.index.month == 3]
        new_data.index = new_data.index.year - 1
        new_data.index = pd.to_datetime((new_data.index).astype(str))

        if new_data.empty:
            new_data = raw_data_1
        elif new_data.index[-1].year != raw_data_year:
            new_data = pd.concat([new_data, raw_data_1], ignore_index=False)

        return new_data
