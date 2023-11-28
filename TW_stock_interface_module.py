import os
import time
import asyncio
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as m_ticker

from datetime import datetime
from tkinter import Tk, Button, Label, StringVar, W, E, N, S, Frame, BooleanVar, Checkbutton, CENTER, NO
from tkinter import ttk, filedialog
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.pylab import mpl

from TW_stock_module import SystemProcessor, TWStockRetrieveModule, FinancialAnalysis, SelectStock, CrawlerProcessor
from TW_stock_base_frame import BaseScrapperFrame, BaseTemplateFrame, msg_queue
from utils import call_by_async


class StockApp(Tk):
    def __init__(self):
        super().__init__()
        self.title("TW STOCK")
        self.configure(background="light yellow")
        self.geometry("960x680")
        # self.resizable(height=False, width=False)
        self._frame = None
        self.switch_frame(StartPage)

    def switch_frame(self, frame_class):
        new_frame = frame_class(self)
        if self._frame is not None:
            self._frame.destroy()
        self._frame = new_frame
        self._frame.pack()


class StartPage(Frame):
    def __init__(self, master):
        global db_path, sys_processor
        super().__init__(master)
        Frame.configure(self, bg='pink')
        
        # 設置資料庫位置
        self.db_path_lbl = Label(self, text="資料庫路徑: ", background="pink", font=("TkDefaultFont", 16))
        self.db_path_lbl.grid(row=0, column=0, sticky=W + E + N + S)
        self.db_path_text = StringVar()

        self.mrsp_btn = Button(self, text="Go to Monthly Report Scrapper",
                               command=lambda: master.switch_frame(MonthlyReportScrapperPage))
        self.mrsp_btn.grid(row=1, column=1)
        self.srsp_btn = Button(self, text="Go to Seasonal Report Scrapper",
                               command=lambda: master.switch_frame(SeasonalReportScrapperPage))
        self.srsp_btn.grid(row=2, column=1)
        self.psp_btn = Button(self, text="Go to Price Scrapper", command=lambda: master.switch_frame(PriceScrapperPage))
        self.psp_btn.grid(row=3, column=1)
        self.frap_btn = Button(self, text="Go to Financial Statement Analysis",
                               command=lambda: master.switch_frame(FinancialReportAnalysisPage))
        self.frap_btn.grid(row=4, column=1)
        self.ssp_btn = Button(self, text="Go to Select Stock App", command=lambda: master.switch_frame(SelectStockPage))
        self.ssp_btn.grid(row=5, column=1)
        self.sap_btn = Button(self, text="Go to Select Stock Analysis App",
                              command=lambda: master.switch_frame(StockAnalysisPage))
        self.sap_btn.grid(row=6, column=1)

        self.db_path = sys_processor.get_latest_path_sql("db") or os.path.join("data", "data.db")
        self.db_path_text.set(self.db_path)

        db_path = self.db_path

        self.db_path_entry = ttk.Entry(self, width=30, textvariable=self.db_path_text)
        self.db_path_entry.grid(row=0, column=1, columnspan=3, sticky=W + E + N + S)
        self.db_path_btn = Button(self, text='請選擇檔案', command=self.get_db_path)
        self.db_path_btn.grid(row=0, column=4, sticky=W + E + N + S)

    # 取得樣板檔案位置
    def get_db_path(self):
        global db_path

        # 獲取文件全路徑
        db_name = filedialog.askopenfilename(title='Select Template',
                                             filetypes=[('.DB', 'db')],
                                             initialdir=os.path.dirname(self.db_path))

        sys_processor.del_path_sql("db", self.db_path_entry.get())
        self.db_path_entry.delete(0, 'end')
        self.db_path_entry.insert(0, db_name)

        sys_processor.save_path_sql(db_name)
        db_path = db_name

    def btn_switch(self, disable=False):
        if disable:
            self.mrsp_btn["state"] = "disabled"
            self.srsp_btn["state"] = "disabled"
            self.psp_btn["state"] = "disabled"
            self.frap_btn["state"] = "disabled"
            self.ssp_btn["state"] = "disabled"
            self.sap_btn["state"] = "disabled"
        else:
            self.mrsp_btn["state"] = "normal"
            self.srsp_btn["state"] = "normal"
            self.psp_btn["state"] = "normal"
            self.frap_btn["state"] = "normal"
            self.ssp_btn["state"] = "normal"
            self.sap_btn["state"] = "normal"


class MonthlyReportScrapperPage(BaseScrapperFrame):
    def __init__(self, master):
        super().__init__(master, "月報", db_path, StartPage, "monthly_revenue", async_loop)


class SeasonalReportScrapperPage(BaseScrapperFrame):
    def __init__(self, master):
        super().__init__(master, "季報", db_path, StartPage, "finance_statement", async_loop)


class PriceScrapperPage(BaseScrapperFrame):
    def __init__(self, master):
        super().__init__(master, "價位", db_path, StartPage, "price", async_loop)


class FinancialReportAnalysisPage(BaseTemplateFrame):
    def __init__(self, master):
        super().__init__(master, sys_processor, "directory", StartPage, async_loop)
        self.symbol_text = StringVar()
        self.symbol_list = self._get_files_id()[0]
        self.symbol_list.insert(0, "all")
        self.symbol_combo = ttk.Combobox(self, textvariable=self.symbol_text, values=self.symbol_list,
                                         postcommand=self.update_func)
        self.exec_combo = ttk.Combobox(self,
                                       values=["all", "更新月報", "更新季報", "更新PER與今日價位", "更新股東占比"])

        self.create_template_widget()
        self.create_updator_widgets()
        self.create_common_widgets()

    def create_updator_widgets(self):
        curr_size = self.grid_size()
        # 欲更新財報分析excel編號設定、執行的項目
        symbol_label = Label(self, text="Symbol: ", background="pink", font=("TkDefaultFont", 16))
        symbol_label.grid(row=curr_size[-1] + 1, column=0, sticky=W)
        self.symbol_combo.grid(row=curr_size[-1] + 1, column=1, sticky=W)
        # 選擇要執行的項目
        exec_label = Label(self, text="執行項目: ", background="pink", font=("TkDefaultFont", 16))
        exec_label.grid(row=curr_size[-1] + 2, column=0, sticky=W)
        self.exec_combo.grid(row=curr_size[-1] + 2, column=1, sticky=W)
        execution_btn = Button(self, text="Execute", command=self.execute_func)
        execution_btn.grid(row=curr_size[-1] + 2, column=2, sticky=W)

    # 更新股票代號
    def update_func(self):
        self.path = self.path_combo.get()
        symbol = self._get_files_id()[0]
        symbol.insert(0, "all")
        self.symbol_combo['values'] = symbol

    # 顯示執行項目
    @call_by_async
    async def execute_func(self):
        job = self.exec_combo.get()
        symbol = self.symbol_text.get()
        files_id, files_id_to_path = self._get_files_id()

        if symbol == "all":
            stock_id_list = files_id
        else:
            stock_id_list = str(symbol).replace(" ", ",").split(",")
            stock_id_list = [i for i in stock_id_list if i.isdigit()]

        folder_path = os.path.join(self.path_text.get(), "自選新增")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        for stock_id in stock_id_list:
            if stock_id not in files_id:
                self.save_excel(stock_id, folder=folder_path)
                file_path = os.path.join(folder_path, f"O_{stock_id}_財報分析.xlsx")
                await asyncio.sleep(0.5)
            else:
                file_path = files_id_to_path[stock_id]
            msg_queue.put("開始更新 {}".format(stock_id))
            print("開始更新 {}".format(stock_id))
            await self._execute_finance_analysis(job, stock_id, file_path)

        msg_queue.put("財報更新完成")

    async def _execute_finance_analysis(self, job, stock_id, file_path):
        fsa = FinancialAnalysis(db_path, msg_queue, file_path)

        _all_works = [
            fsa.update_monthly_report,
            fsa.update_season_report,
            fsa.update_cash_flow,
        ]
        if job == "更新月報":
            _all_works = [
                fsa.update_monthly_report,
            ]
        elif job == "更新季報":
            _all_works = [
                fsa.update_season_report,
                fsa.update_cash_flow,
            ]
        elif job == "更新PER與今日價位":
            _all_works = []
        elif job == "更新股東占比":
            _all_works = [fsa.update_directors_and_supervisors]
        _all_works.extend(
            [
                fsa.update_price_today,
                fsa.update_per,
            ]
        )
        # await fsa.update_season_report(stock_id)
        try:
            for work in _all_works:
                await work(stock_id)
        except Exception as e:
            msg_queue.put("{}發生問題，問題原因: {}".format(stock_id, e))
            print("{}發生問題，問題原因: {}".format(stock_id, e))

    def clear_func(self):
        super().clear_func()
        self.symbol_combo.delete(0, "end")
        self.exec_combo.delete(0, "end")


class SelectStockPage(BaseTemplateFrame):
    def __init__(self, master):
        super().__init__(master, sys_processor, "select_stock_directory", StartPage, async_loop)
        self.selected_stock = []

        # 用於儲存sql
        self.chk_list = []
        self.chk_var_list = []
        self.combo_list = []
        self.entry_list = []
        self.content_list = []

        # 需取得的元件資料
        self.start = None
        self.end = None
        self.period = None
        self.sp_chk_var = None
        self.sl_chk_var = None
        self.sp_entry = None
        self.sl_entry = None

        self.component_list = []
        self.create_condition_list = [
            "市值", "三年自由現金流", "股東權益報酬率", "營業利益年成長率", "八季營益率變化", "市值營收比",
            "短期營收年增", "短期營收年增2", "短期淨利年增", "存貨周轉變化率", "rsv"
        ]

        # 設置選取樣板的資料夾及檔案按鈕，並取得路徑
        self.create_template_widget()

        self._create_select_stock_widgets()
        self._create_backtest_widgets()

        # 顯示更新動作進度
        self.create_common_widgets()

        conn = sqlite3.connect(db_path)
        self.crawler_processor = CrawlerProcessor(conn, msg_queue)
        self.update_func()

    def _create_select_stock_widgets(self):
        curr_size = self.grid_size()

        # 選取欲使用的條件以及其設定值
        Label(self, text="選股條件:", bg="red", font=("TkDefaultFont", 14)).grid(row=curr_size[-1] + 1, column=0,
                                                                                 columnspan=6, sticky=W + E)
        start_label = Label(self, text="選股日期:", bg="pink", font=("TkDefaultFont", 12))
        start_label.grid(row=curr_size[-1] + 2, column=0, sticky=W + E)
        start_var1 = StringVar()
        start = ttk.Entry(self, textvariable=start_var1, font=("TkDefaultFont", 12))
        start.grid(row=curr_size[-1] + 2, column=1, columnspan=3, sticky=W)

        row, col = curr_size[-1] + 3, 0
        for i, cond in enumerate(self.create_condition_list):
            row = curr_size[-1] + 3 + i // 2
            col = 0 if not (i % 2) else 3

            entry_var = StringVar()
            entry = ttk.Entry(self, textvariable=entry_var, width=15, font=("TkDefaultFont", 12))
            entry.grid(row=row, column=col + 2, sticky=W)
            combo = ttk.Combobox(self, width=4, values=[">", ">=", "=", "<", "<="])
            combo.grid(row=row, column=col + 1, sticky=W)

            chk_var = BooleanVar()
            chk = Checkbutton(self, variable=chk_var, bg="pink", text=cond, font=("TkDefaultFont", 12))
            chk.grid(row=row, column=col, sticky=E)

            self.component_list.append((chk, chk_var, combo, entry))

        # 選擇要執行的項目
        if col == 0:
            row, col = row, 3
        else:
            row, col = row + 1, 0
        execution_btn = Button(self, text="Execute", command=self.execute_func)
        execution_btn.grid(row=row, column=col, sticky=W + E)
        save_btn = Button(self, text="Save excel", command=self._show_result_and_handle_excel)
        save_btn.grid(row=row, column=col + 1, sticky=W + E)

        self.component_list.append((start_label, False, None, start))
        self.start = start

    def _create_backtest_widgets(self):
        curr_size = self.grid_size()
        row = curr_size[-1] + 1

        # 回測設置
        Label(self, text="回測設定:", bg="red", font=("TkDefaultFont", 14)).grid(row=row, column=0, columnspan=6,
                                                                                 sticky=W + E)

        # 回測的起始時間
        end_label = Label(self, text="回測起始日期:", bg="pink", font=("TkDefaultFont", 12))
        end_label.grid(row=row + 1, column=0, sticky=W + E)
        end_var1 = StringVar()
        end = ttk.Entry(self, textvariable=end_var1, font=("TkDefaultFont", 12))
        end.grid(row=row + 1, column=1, columnspan=2, sticky=W)

        # 多少週期更新一次
        period_label = Label(self, text="週期天數:", bg="pink", font=("TkDefaultFont", 12))
        period_label.grid(row=row + 1, column=3, sticky=W + E)
        period_var1 = StringVar()
        period = ttk.Entry(self, textvariable=period_var1, font=("TkDefaultFont", 12))
        period.grid(row=row + 1, column=4, columnspan=2, sticky=W)

        # 是否停利
        sp_var = StringVar()
        sp_entry = ttk.Entry(self, textvariable=sp_var, font=("TkDefaultFont", 12))
        sp_entry.grid(row=row + 2, column=1, columnspan=2, sticky=W)
        sp_chk_var = BooleanVar()
        sp_chk = Checkbutton(self, variable=sp_chk_var, bg="pink", text="停利", font=("TkDefaultFont", 12))
        sp_chk.grid(row=row + 2, column=0, sticky=W + E)

        # 是否停損
        sl_var = StringVar()
        sl_entry = ttk.Entry(self, textvariable=sl_var, font=("TkDefaultFont", 12))
        sl_entry.grid(row=row + 2, column=4, columnspan=2, sticky=W)
        sl_chk_var = BooleanVar()
        sl_chk = Checkbutton(self, variable=sl_chk_var, bg="pink", text="停損", font=("TkDefaultFont", 12))
        sl_chk.grid(row=row + 2, column=3, sticky=W + E)

        # 執行回測
        backtest_btn = Button(self, text='執行回測', command=self.backtest_func)
        backtest_btn.grid(row=row + 2, column=6, sticky=W + E)

        self.component_list += [
            (end_label, False, None, end,),
            (period_label, False, None, period,),
            (sp_chk, sp_chk_var, None, sp_entry,),
            (sl_chk, sl_chk_var, None, sl_entry,),
        ]
        self.end = end
        self.period = period
        self.sp_chk_var = sp_chk_var
        self.sl_chk_var = sl_chk_var
        self.sp_entry = sp_entry
        self.sl_entry = sl_entry

    # 選定後，自動帶入上次執行成功的條件
    def _save_select_stock_condition(self):
        for chk, chk_var, combo, entry in self.component_list:
            self.chk_list.append(chk.cget("text"))
            self.chk_var_list.append(chk_var.get() if chk_var else "")
            self.combo_list.append(combo.get() if combo else "")
            self.entry_list.append(entry.get())
            content = f"""{chk.cget("text")} {combo.get()} {entry.get()}""" if combo else None
            self.content_list.append(content)

        sys_processor.save_select_stock_cache_to_sql(
            (self.chk_list, self.chk_var_list, self.content_list, self.combo_list, self.entry_list))

    # 顯示執行項目
    @call_by_async
    async def execute_func(self):
        msg_queue.put("開始執行")

        conn = sqlite3.connect(db_path)
        select_stock = SelectStock(conn, msg_queue)

        msg_queue.put("連接上db")
        self._save_select_stock_condition()

        msg_queue.put("儲存完選股條件及路徑資料")

        date = datetime.strptime(self.start.get(), "%Y-%m-%d")
        activate_list = self.chk_var_list[:len(self.create_condition_list)]
        cond_content_list = self.content_list[:len(self.create_condition_list)]
        # cond_content_list = list(filter(None, cond_content_list))

        print("開始選股")
        msg_queue.put("開始選股")
        result = await select_stock.my_strategy(date=date, cond_content=cond_content_list, activate=activate_list)
        self.selected_stock = list(result.index)

        msg_queue.put("符合選擇條件的股票有: {}\n".format(self.selected_stock))

    # 回測功能
    @call_by_async
    async def backtest_func(self):
        msg_queue.put("開始執行")
        conn = sqlite3.connect(db_path)
        select_stock = SelectStock(conn, msg_queue)
        self._save_select_stock_condition()
        msg_queue.put("連接上db, 開始執行回測")

        start = datetime.strptime(self.end.get(), "%Y-%m-%d")
        end = datetime.strptime(self.start.get(), "%Y-%m-%d")
        period = int(self.period.get())
        activate_list = self.chk_var_list[:len(self.create_condition_list)]
        cond_content_list = self.content_list[:len(self.create_condition_list)]

        sp = float(self.sp_entry.get()) if self.sp_chk_var.get() else None
        sl = float(self.sl_entry.get()) if self.sl_chk_var.get() else None

        (profit, record, max_profit, min_profit, process) = await select_stock.backtest(
            start, end, period, cond_content_list, activate_list,
            stop_loss=sl, stop_profit=sp
        )

        msg_queue.put('交易利潤 :\n {}\n'.format(profit))
        msg_queue.put('交易紀錄 :\n {}\n'.format(record))
        msg_queue.put("完成")

    # 顯示作業進度
    def update_func(self):
        self.clear_func()
        for (chk, chk_var, combo, entry), cond in zip(self.component_list, self.create_condition_list):
            cache = self.sys_processor.get_select_stock_cache_to_sql(cond)
            chk_var.set(bool(cache["activate"]))
            if chk_var.get():
                entry.insert(0, str(cache["cond_value"]) or "")
                combo.insert(0, str(cache["operator"]) or "")
            else:
                entry.delete(0, "end")
                combo.delete(0, "end")

        sp_cache = self.sys_processor.get_select_stock_cache_to_sql("停利")
        sl_cache = self.sys_processor.get_select_stock_cache_to_sql("停損")
        self.start.insert(0, self.sys_processor.get_select_stock_cache_to_sql("選股日期:")["cond_value"])
        self.end.insert(0, self.sys_processor.get_select_stock_cache_to_sql("回測起始日期:")["cond_value"])
        self.period.insert(0, self.sys_processor.get_select_stock_cache_to_sql("週期天數:")["cond_value"])
        self.sp_chk_var.set(sp_cache["activate"])
        self.sl_chk_var.set(sl_cache["activate"])
        self.sp_entry.insert(0, sp_cache["cond_value"])
        self.sl_entry.insert(0, sl_cache["cond_value"])

    # 清除顯示
    def clear_func(self):
        super().clear_func()
        for chk, chk_var, combo, entry in self.component_list:
            if chk_var:
                chk_var.set(0)
            entry.delete(0, "end")

    def _show_result_and_handle_excel(self):
        existed = self._get_files_id()[0]
        folder_path = os.path.join(self.path, "選股結果")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        msg_queue.put("\n")
        for stock_id in self.selected_stock:
            if stock_id in existed:
                msg_queue.put("{}已存在".format(stock_id))
            else:
                msg_queue.put("新增{}".format(stock_id))
                self.save_excel(stock_id, folder=folder_path)


class StockAnalysisPage(Frame):
    def __init__(self, master):
        Frame.__init__(self, master)
        Frame.configure(self, bg='pink')
        try:
            conn = sqlite3.connect(db_path)
            self.data_getter = TWStockRetrieveModule(conn)
        except Exception as e:
            print("database is not connected: {}".format(e))
            master.switch_frame(StartPage)

        self.prev_id = ""

        self.stock_id_label = Label(self, text="分析股票代號: ", background="pink", font=("TkDefaultFont", 16))
        self.stock_id_label.grid(row=0, column=0, columnspan=3, sticky=W)
        self.stock_id_combo = ttk.Combobox(self, postcommand="", values=["2330", "0050"])
        self.stock_id_combo.current(0)
        self.stock_id_combo.grid(row=0, column=3, columnspan=3, sticky=W)

        self.back_btn = Button(self, text="Go back", command=lambda: master.switch_frame(StartPage))
        self.back_btn.grid(row=1, column=0, sticky=W)
        self.m_report_btn = Button(self, text="月財報", command=lambda: [self.initial_data(),
                                                                         self.show_table(self.month_df),
                                                                         self.create_widget(self.month_fig)])
        self.m_report_btn.grid(row=1, column=1, sticky=W)
        self.s_report_btn = Button(self, text="季財報", command=lambda: [self.initial_data(),
                                                                         self.show_table(self.season_df),
                                                                         self.create_widget(self.season_fig, x=0, y=4,
                                                                                            xs=5)
                                                                         ])
        self.s_report_btn.grid(row=1, column=2, sticky=W)
        self.cash_btn = Button(self, text="現金流", command=lambda: [self.initial_data(),
                                                                     self.show_table(self.cash_df)
                                                                     ])
        self.cash_btn.grid(row=1, column=3, sticky=W)
        self.price_btn = Button(self, text="價位分析", command=lambda: [self.initial_data(),
                                                                        self.show_table(self.est_price),
                                                                        self.create_widget(self.month_fig)
                                                                        ])
        self.price_btn.grid(row=1, column=4, sticky=W)
        self.exit_btn = Button(self, text="Exit", command=self.quit)
        self.exit_btn.grid(row=1, column=5, sticky=W)

    def initial_data(self):
        id = self.stock_id_combo.get()
        if self.prev_id != id:
            # 月財報
            month_setting = {
                "title": "股價/月營收年增",
                "main": ["股價"],
                "sub": ["月營收年增率3個月移動平均", "月營收年增率12個月移動平均"],
                "xlabel": ["日期"],
                "ylabel": ["價位", "增幅(%)"],
            }
            self.month_df = self.data_getter.retrieve_month_data(id)
            fig, setting = self.data_getter.module_data_to_draw(id, month_setting)
            self.month_fig = self.draw_figure(fig, setting)

            # 季財報
            self.season_df = self.data_getter.retrieve_season_data(id)
            self.season_fig = self.draw_figures()

            # 現金流
            self.cash_df = self.data_getter.retrieve_cash_data(id)

            # 預估股價
            self.est_price = self.data_getter.price_estimation(id)

            # 記錄此次分析股票代號
            self.prev_id = self.stock_id_combo.get()

    def create_widget(self, figure, x=7, y=2, xs=1, ys=1, s=W + E + N + S, tool=True):
        self.canvas = FigureCanvasTkAgg(figure, self)
        self.canvas.draw()
        self.canvas.get_tk_widget().grid(row=y, column=x, sticky=s, rowspan=ys, columnspan=xs)

        # 把matplotlib繪製圖形的導航工具欄顯示到tkinter視窗上
        if tool:
            toolbar = NavigationToolbar2Tk(self.canvas, self, pack_toolbar=False)
            toolbar.grid(row=y + 1, column=x, sticky=W + E)
            # self.canvas._tkcanvas.grid(row=y-1, column=x, sticky=s)

    def draw_figure(self, df, setting):
        """建立繪圖物件"""

        # 設定中文顯示字型
        mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  # 中文顯示
        mpl.rcParams['axes.unicode_minus'] = False  # 負號顯示

        # 建立繪圖物件f figsize的單位是英寸 畫素 = 英寸*解析度
        figure = plt.figure(num=1, figsize=(10, 10), dpi=90, facecolor="pink", edgecolor='green', frameon=True)

        # 建立一副子圖
        fig, ax1 = plt.subplots(figsize=(4, 4), constrained_layout=False)  # 三個引數，依次是：行，列，當前索引
        plt.subplots_adjust(left=0.1, right=0.9, bottom=0.15, top=0.9)

        secondary_y = False
        x = df.index
        color_list = ["red", "green", "blue", "pink", "orange"]
        for col_name, color in zip(df.columns, color_list):
            axis = col_name.split("*")
            if axis[0] == "m":
                ax1.plot(x, df[col_name], color=color, label=axis[1])
            else:
                if not secondary_y:
                    ax2 = ax1.twinx()
                ax2.plot(x, df[col_name], color=color, label=axis[1], linestyle="--")
                secondary_y = True

        ax1.set_title(setting.get("title", ""), loc='center', pad=5, fontsize='large', color='red')  # 設定標題
        # 定義legend 重新定義了一次label
        line, label = ax1.get_legend_handles_labels()

        # ,fontsize='xx-large'
        ax1.set_xlabel(setting.get("xlabel", [""])[0])  # 確定座標軸標題
        ax1.xaxis.set_label_coords(0, -0.05)
        tick_spacing = x.size / 10  # x軸密集度
        ax1.xaxis.set_major_locator(m_ticker.MultipleLocator(tick_spacing))
        ax1.tick_params('x', labelrotation=70)

        ax1.set_ylabel(setting.get("ylabel", [""])[0], rotation=0)
        ax1.yaxis.set_label_coords(0, 1.02)

        if secondary_y:
            ax2.set_ylabel(setting.get("ylabel", ["", ""])[1], rotation=0)
            ax2.yaxis.set_label_coords(1, 1.05)
            line2, label2 = ax2.get_legend_handles_labels()
            line += line2
            label += label2

        ax1.grid(which='major', axis='x', color='gray', linestyle='-', linewidth=0.5, alpha=0.2)  # 設定網格
        ax1.invert_xaxis()
        ax1.legend(line, label, loc=0)

        return fig

    def draw_figures(self):
        """建立繪圖物件"""
        # 設定中文顯示字型
        mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  # 中文顯示
        mpl.rcParams['axes.unicode_minus'] = False  # 負號顯示

        figure = plt.figure(num=8, figsize=(5, 2), dpi=80, facecolor="gold", edgecolor='green', frameon=True)
        draw_df = self.season_df.set_index("項目").loc[[
            "營業利益率", "應收帳款週轉率", "存貨周轉率", "存貨營收比",
            "股東權益報酬率(年預估)", "稅後淨利率(累計)", "總資產週轉率(次/年)", "權益係數"]]

        nrows, ncols = 2, 4
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols)
        count = 0
        for r in range(nrows):
            for c in range(ncols):
                df = draw_df.iloc[count]
                df.plot(ax=axes[r, c])
                axes[r, c].set_title(df.name, loc='center', color='red', pad=5)
                axes[r, c].invert_xaxis()
                count += 1

        # plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)
        # fig.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)

        return fig

    def show_table(self, df):
        def fixed_map(option):
            return [elm for elm in style.map('Treeview', query_opt=option) if
                    elm[:2] != ('!disabled', '!selected')]

        style = ttk.Style()
        style.map('Treeview', foreground=fixed_map('foreground'), background=fixed_map('background'))

        self.data_table = ttk.Treeview(self, columns=("Tags"), height=15)
        self.data_table.grid(row=2, column=0, columnspan=5, sticky=W + E)

        vsb = ttk.Scrollbar(self, orient="vertical", command=self.data_table.yview)
        vsb.grid(column=6, row=2, rowspan=2, sticky=N + S)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.data_table.xview)
        hsb.grid(column=0, row=3, columnspan=5, sticky=W + E)
        self.data_table.configure(yscrollcommand=vsb.set)
        self.data_table.configure(xscrollcommand=hsb.set)

        self.data_table.column("#0", width=0, stretch=NO)
        self.data_table.heading("#0", text="", anchor=CENTER)

        df_col = df.columns.values
        df_row = df.index.values
        counter = len(df_col)
        self.data_table['columns'] = tuple(df_col)

        # 建立欄位名
        for n in range(counter):
            title = df_col[n]
            self.data_table.column(title, width=55, stretch=NO, anchor=CENTER)
            self.data_table.heading(title, text=title, anchor=CENTER)

        # 建立數值至表格中
        self.data_table.tag_configure('highlight', background='#DD99FF')
        for m in range(len(df_row)):
            value = tuple(df.iloc[m].replace(['NaN', 'nan', np.nan], "").tolist())
            if value[0][0] == "*":
                self.data_table.insert(parent='', index='end', text='', values=value, tag='highlight', open=False)
            else:
                self.data_table.insert(parent='', index='end', text='', values=value)


if __name__ == "__main__":
    sys_db_path = os.path.join("data", "system_record.json")
    sys_processor = SystemProcessor(sys_db_path)

    db_path = ""

    async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(async_loop)

    root = StockApp()
    root.mainloop()

