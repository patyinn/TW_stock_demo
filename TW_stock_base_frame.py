import os
import time
import asyncio
import threading
import multiprocessing
import sqlite3

from queue import Queue
from datetime import datetime
from openpyxl import load_workbook

from tkinter import Button, Label, StringVar, W, E, N, S, NSEW, Frame
from tkinter import ttk, scrolledtext, WORD, END, filedialog

from TW_stock_module import CrawlerProcessor
from utils import call_by_async

msg_queue = Queue()


class BaseFrame(Frame):
    def __init__(self, master, start_page, async_loop):
        super().__init__(master)
        Frame.configure(self, bg='pink')
        self.scroll_txt = scrolledtext.ScrolledText(self, wrap=WORD, height=16, width=40)
        self.start_page = start_page
        self.msg_flag = True
        self.async_loop = async_loop

    def create_common_widgets(self):
        col, row = self.grid_size()

        # 顯示更新動作進度
        self.scroll_txt.grid(row=row+1, column=0, columnspan=col, sticky=W + E + N + S + NSEW, padx=20,
                             pady=30)

        # 返回主頁面、更新、清除、離開程式
        back_btn = Button(self, text="Go back", command=self.go_back_func)
        back_btn.grid(row=row + 2, column=0, sticky=W+E)
        update_btn = Button(self, text="Update message", command=self.update_func)
        update_btn.grid(row=row + 2, column=1, sticky=W+E)
        clear_btn = Button(self, text="Clear message", command=self.clear_func)
        clear_btn.grid(row=row + 2, column=3, sticky=W+E)
        exit_btn = Button(self, text="Exit Application", command=self.quit)
        exit_btn.grid(row=row + 2, column=4, sticky=W+E)

    @property
    def msg_flag(self):
        return self._msg_flag

    @msg_flag.setter
    def msg_flag(self, value: bool):
        self._msg_flag = value
        if self._msg_flag:
            # Turn-on the worker thread.
            threading.Thread(target=self.handle_message, daemon=True).start()
        else:
            self._clear_queue()

    def handle_message(self):
        _pbar_line = 0
        while self._msg_flag:
            if not msg_queue.empty():
                msg = msg_queue.get()
                if isinstance(msg, tuple):
                    insert_position = self.scroll_txt.index(END)
                    line, column = map(int, insert_position.split("."))
                    if _pbar_line:
                        del_line = line-_pbar_line
                        self.scroll_txt.delete(f"end-{del_line+1}l", f"end-{del_line}l")
                        _pbar_line = line - 1
                    else:
                        _pbar_line = line
                    self.scroll_txt.insert(END, f"{msg[0]}\n")
                else:
                    self.scroll_txt.insert(END, f"{msg}\n")
                msg_queue.task_done()

            # 繼續定期檢查
            self.update()
            time.sleep(0.1)
            # self.after(1000, self.handle_message)

    def _clear_queue(self):
        while not msg_queue.empty():
            msg_queue.get()

    # 回到主頁面，關閉線程
    def go_back_func(self):
        self.msg_flag = False
        self._clear_queue()
        self.master.switch_frame(self.start_page)

    # 顯示作業進度
    def update_func(self):
        pass

    # 顯示執行項目
    def execute_func(self):
        pass

    # 清除顯示
    def clear_func(self):
        self.scroll_txt.delete(1.0, "end")


class BaseScrapperFrame(BaseFrame):
    def __init__(self, master, mode, db_path,  start_page, table_name, async_loop):
        super().__init__(master, start_page, async_loop)
        self.master = master
        self.mode = mode
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path)
        self.crawler_processor = CrawlerProcessor(conn, msg_queue)

        self.table_name = table_name
        self.to_date_combo = ttk.Combobox(self, postcommand=lambda: self.fr_date_combo.configure(
            values=self.crawler_processor.date_func(table=self.table_name, pattern="from")))
        self.fr_date_combo = ttk.Combobox(self, postcommand=lambda: self.to_date_combo.configure(
            values=self.crawler_processor.date_func(table=self.table_name, pattern="to")))
        self.create_crawler_widgets()
        self.create_common_widgets()

    def create_crawler_widgets(self):
        # 選擇要爬取的資料型態
        title_label = Label(self, text=f"{self.mode}爬取: ", background="pink", font=("TkDefaultFont", 16))
        title_label.grid(row=0, column=0, columnspan=2, sticky=W)
        fr_date_label = Label(self, text="From: ", background="pink", font=("TkDefaultFont", 16))
        fr_date_label.grid(row=1, column=0, sticky=W)
        self.fr_date_combo.grid(row=1, column=1, sticky=W)

        to_date_label = Label(self, text="To: ", background="pink", font=("TkDefaultFont", 16))
        to_date_label.grid(row=1, column=2, sticky=W)
        self.to_date_combo.grid(row=1, column=3, sticky=W)

        execution_btn = Button(self, text="Execute", command=self.execute_func)
        execution_btn.grid(row=1, column=4, sticky=W)

    # 顯示作業進度
    def update_func(self):
        to_date = self.crawler_processor.date_func(table=self.table_name, pattern="to")
        from_date = self.crawler_processor.date_func(table=self.table_name, pattern="from")
        self.fr_date_combo.set(from_date)
        self.to_date_combo.set(to_date)

    # 顯示執行項目
    @call_by_async
    async def execute_func(self):
        from_date = self.fr_date_combo.get()
        from_date = str(from_date.replace(" ", "-"))
        from_date = datetime.strptime(from_date, '%Y-%m-%d')

        to_date = self.to_date_combo.get()
        to_date = str(to_date.replace(" ", "-"))
        to_date = datetime.strptime(to_date, '%Y-%m-%d')

        cmd = (from_date, to_date)

        msg_queue.put("正在爬取從 {} 至 {} 周期間的 {}".format(cmd[0], cmd[1], self.table_name))
        print("正在爬取從 {} 至 {} 周期間的 {}".format(cmd[0], cmd[1], self.table_name))

        conn = sqlite3.connect(self.db_path)
        crawler_processor_for_thread = CrawlerProcessor(conn, msg_queue)
        task = asyncio.create_task(crawler_processor_for_thread.exec_func(self.table_name, cmd[0], cmd[1]))
        await task

        msg_queue.put("完成爬取從 {} 至 {} 周期間的 {}".format(cmd[0], cmd[1], self.table_name))
        print("完成爬取從 {} 至 {} 周期間的 {}".format(cmd[0], cmd[1], self.table_name))

    # 清除顯示
    def clear_func(self):
        super().clear_func()
        self.fr_date_combo.delete(0, "end")
        self.to_date_combo.delete(0, "end")


class BaseTemplateFrame(BaseFrame):
    def __init__(self, master, sys_processor, directory_type, start_page, async_loop):
        super().__init__(master, start_page, async_loop)
        self.sys_processor = sys_processor
        self.directory_type = directory_type
        self.template_path_text = StringVar()
        self.template_path = self.sys_processor.get_latest_path_sql("file") or os.path.abspath('')
        self.template_path_text.set(self.template_path)

        self.template_path_combo = ttk.Combobox(self, width=70, textvariable=self.template_path_text,
                                                postcommand=lambda: self.template_path_combo.configure(
                                                    values=self.sys_processor.get_latest_path_sql("file")))
        self.path_text = StringVar()
        self.path = self.sys_processor.get_latest_path_sql("directory") or os.path.abspath('')
        self.path_text.set(self.path)
        self.path_combo = ttk.Combobox(self, width=70, textvariable=self.path_text,
                                       postcommand=lambda: self.path_combo.configure(
                                           values=self.sys_processor.get_latest_path_sql("directory")))

    def create_template_widget(self):
        # 設置選取樣板的資料夾及檔案按鈕，並取得路徑
        template_path_lbl = Label(self, text="樣板路徑: ", background="pink", font=("TkDefaultFont", 16))
        template_path_lbl.grid(row=0, column=0, sticky=W + E + N + S)
        self.template_path_combo.grid(row=0, column=1, columnspan=3, sticky=W + E + N + S)
        template_path_btn = Button(self, text='請選擇檔案', command=self.get_template_path)
        template_path_btn.grid(row=0, column=4, sticky=W + E + N + S)
        del_template_path_btn = Button(self, text='刪除紀錄', command=self.del_template)
        del_template_path_btn.grid(row=0, column=5, sticky=W + E + N + S)

        # 設置選取要更新的資料夾與檔案按鈕，並取得路徑
        path_lbl = Label(self, text="資料夾路徑: ", background="pink", font=("TkDefaultFont", 16))
        path_lbl.grid(row=1, column=0, sticky=W + E + N + S)
        self.path_combo.grid(row=1, column=1, columnspan=3, sticky=W + E + N + S)
        path_btn = Button(self, text='請選擇檔案', command=self.get_path)
        path_btn.grid(row=1, column=4, sticky=W + E + N + S)
        del_path_btn = Button(self, text='刪除紀錄', command=self.del_path)
        del_path_btn.grid(row=1, column=5, sticky=W + E + N + S)

    # 刪除已儲存的樣板路徑
    def del_template(self):
        if self.template_path_combo.get():
            path = self.template_path_combo.get()
            self.sys_processor.del_path_sql("file", path)

    # 取得樣板檔案位置
    def get_template_path(self):
        # 獲取文件全路徑
        filename = filedialog.askopenfilename(title='Select Template',
                                              filetypes=[('.XLSX', 'xlsx'), ('All Files', '*')],
                                              initialdir=os.path.dirname(self.template_path))
        self.template_path_combo.delete(0, 'end')
        self.template_path_combo.insert(0, filename)

        self.sys_processor.save_path_sql(filename)

    # 欲更新檔案位置
    def get_path(self):
        directory = filedialog.askdirectory(title='Select directory',
                                            initialdir=self.path)
        self.path_combo.delete(0, 'end')
        self.path_combo.insert(0, directory)
        if self.directory_type == "directory":
            self.sys_processor.save_path_sql(directory)
        else:
            self.sys_processor.save_path_sql(directory, source="select_stock")

    # 刪除已儲存的資料夾路徑
    def del_path(self):
        if self.path_combo.get():
            path = self.path_combo.get()
            self.sys_processor.del_path_sql(self.directory_type, path)

    # 以樣板儲存新檔案
    def save_excel(self, stock_id, folder):
        path = self.template_path_text.get()
        wb = load_workbook(path)
        new_path = os.path.join(folder, f"O_{stock_id}_財報分析.xlsx")
        wb.save(new_path)

    # 取得現有檔案的代號
    def _get_files_id(self):
        file = self.sys_processor.show_folder_content(self.path, prefix="O_", postfix=".xlsx")
        index, dictionary = [], {}
        for num in file[0:]:
            idx = ''.join([x for x in num if x.isdigit()])
            dictionary[idx] = num
            index.append(idx)
        return index, dictionary
