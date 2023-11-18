import os

from datetime import datetime
from openpyxl import load_workbook

from tkinter import Tk, Button, Label, StringVar, W, E, N, S, NSEW, Frame, BooleanVar, Checkbutton, CENTER, NO
from tkinter import ttk, scrolledtext, WORD, INSERT, filedialog


class BaseFrame(Frame):
    def __init__(self, master, start_page):
        super().__init__(master)
        Frame.configure(self, bg='pink')
        self.scroll_txt = scrolledtext.ScrolledText(self, wrap=WORD, height=16, width=40)
        self.start_page = start_page

    def create_common_widgets(self):
        col, row = self.grid_size()

        # 顯示更新動作進度
        self.scroll_txt.grid(row=row+1, column=0, columnspan=col, sticky=W + E + N + S + NSEW, padx=20,
                             pady=30)

        # 返回主頁面、更新、清除、離開程式
        back_btn = Button(self, text="Go back", command=lambda: self.master.switch_frame(self.start_page))
        back_btn.grid(row=row + 2, column=0, sticky=W+E)
        update_btn = Button(self, text="Update message", command=self.update_func)
        update_btn.grid(row=row + 2, column=1, sticky=W+E)
        clear_btn = Button(self, text="Clear message", command=self.clear_func)
        clear_btn.grid(row=row + 2, column=3, sticky=W+E)
        exit_btn = Button(self, text="Exit Application", command=self.quit)
        exit_btn.grid(row=row + 2, column=4, sticky=W+E)

    # 顯示作業進度
    def update_func(self):
        pass

    # 顯示執行項目
    def execute_func(self):
        pass

    # 清除顯示
    def clear_func(self):
        pass


class BaseScrapperFrame(BaseFrame):
    def __init__(self, master, mode, start_page, crawler_processor, table_name):
        super().__init__(master, start_page)
        self.master = master
        self.mode = mode
        self.crawler_processor = crawler_processor
        self.table_name = table_name
        self.to_date_combo = ttk.Combobox(self, postcommand=lambda: self.fr_date_combo.configure(
            values=self.crawler_processor.date_func(table=self.table_name, pattern="F")))
        self.fr_date_combo = ttk.Combobox(self, postcommand=lambda: self.to_date_combo.configure(
            values=self.crawler_processor.date_func(table=self.table_name, pattern="T")))
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

        execution_btn = Button(self, text="Execute", comman=self.execute_func)
        execution_btn.grid(row=1, column=4, sticky=W)

    # 顯示作業進度
    def update_func(self):
        to_date = self.crawler_processor.date_func(table=self.table_name, pattern="T")
        from_date = self.crawler_processor.date_func(table=self.table_name, pattern="F")

        self.fr_date_combo['values'] = from_date
        self.to_date_combo['values'] = to_date

    # 顯示執行項目
    def execute_func(self):
        from_date = self.fr_date_combo.get()
        from_date = str(from_date.replace(" ", "-"))
        from_date = datetime.strptime(from_date, '%Y-%m-%d')

        to_date = self.to_date_combo.get()
        to_date = str(to_date.replace(" ", "-"))
        to_date = datetime.strptime(to_date, '%Y-%m-%d')

        cmd = (from_date, to_date)

        self.scroll_txt.insert(INSERT, "正在爬取從 {} 至 {} 周期間的 {}\n".format(cmd[0], cmd[1], self.table_name))
        self.update()
        self.after(1000)
        self.fr_date_combo.delete(0, "end")
        self.to_date_combo.delete(0, "end")
        self.crawler_processor.exec_func(self.table_name, cmd[0], cmd[1])
        self.scroll_txt.insert(INSERT, "完成爬取從 {} 至 {} 周期間的 {}\n".format(cmd[0], cmd[1], self.table_name))
        self.update()
        self.after(1000)

    # 清除顯示
    def clear_func(self):
        self.scroll_txt.delete(1.0, "end")
        self.fr_date_combo.delete(0, "end")
        self.to_date_combo.delete(0, "end")


class BaseTemplateFrame(BaseFrame):
    def __init__(self, master, sys_processor, directory_type, start_page):
        super().__init__(master, start_page)
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
        del_path_btn = Button(self, text='刪除紀錄', command=self.del_path(category=self.directory_type))
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

    # 欲更新檔案位置
    def get_path(self):
        directory = filedialog.askdirectory(title='Select directory',
                                            initialdir=self.path)
        self.path_combo.delete(0, 'end')
        self.path_combo.insert(0, directory)

    # 刪除已儲存的資料夾路徑
    def del_path(self, category):
        if self.path_combo.get():
            path = self.path_combo.get()
            self.sys_processor.del_path_sql(category, path)

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