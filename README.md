# tkinter 台股爬財報程式

* [NOTE] 本程式部分功能需要finlab線上課程所提供的程式，並且加以修改，由於並非完全自己做的，因此finlab程式無法提供。

## 主介面與功能:
1. 採用python的圖形化介面tkinter，可選擇多項功能，主要的介面功能包含:
   1. 爬取台股季、月報、股價等相關資料、
   2. 更新於個人使用的excel，是固定的格式、
   3. 使用filab課程的回測系統顯示選股策略，或是將選股結果儲存成分析表格、
   4. 將excel股票分析需要的資料用treeview顯示於介面上，也包含一些分析圖表、也可以在選單中選擇最新一次選股策略的結果。
2. 利用多線程，以異步的形式執行後台工作，或是特別一條線程來顯示當前工作進度等相關資訊於介面上，使用上就比較不會卡頓。
3. 可儲存一些常用路徑、最新一次的選股條件於json檔案，當作系統快取，以便快速運用。
4. 主頁面可以指定db，db是採用sqlite，剛開始啟動時會有預設路徑。

## 介面功能詳細介紹:

### 1. 爬取季、月報、股價功能:
   * 這三者的主要介面相同，包含選取時間區，並抓取該區間的資料，訊息欄(scrolled_text)，清除訊息、更新資料庫、離開、結束程式按鈕。
   * 時間區間會以db的最新一筆時間到當天作為預設，更新資料庫按鈕可以重新抓取資料庫的最新資料。
   * 主要使用finlab的爬蟲程式並加以修改、其提供的程式包含資料爬取、清洗、儲存功能。
   * 為了配合tkinter響應，部分程式改為異步，並且在爬取時開啟多線程池，目的是處理一些簡單工作，像是requests等待、文件處理等等。
   * 為了讓前台顯示爬取進度，使用一個Queue來用作訊息儲存，將要拋出到前台的訊息存至佇列，會有函式(所有繼承的tkinter frame)定期會將佇列內容顯示於前台的訊息框中。
   * 訊息框顯示最多20行資料，如果有progress bar會刪除舊的更新進度。
   * 爬取季報會儲存清洗完資料的pickle檔，如果步手動刪除的話，後面有重複的日期會抓取檔案內容來更新。

### 2. 更新excel功能:
   * 主要介面包含excel樣板及儲存路徑選擇、要更新的股票id及動作，訊息欄(scrolled_text)，清除訊息、更新資料庫、離開、結束程式按鈕。
   * 程式會自動抓取儲存路徑內所有含有股票id的excel，顯示於欲更新股票id的下拉選單(combobox)，也可以輸入一個新的id，程式會根據id自動建立一個新的excel檔案於儲存路徑上。
   * 可以更新excel的功能，包含全部、月報、季報、價位的資料更新，皆使用異步來執行。
   * 根據選擇不同的功能以及excel起始日期，使用pandas抓取相關的資料處理後，再使用openpyxl將資料更新到excel上，或是從樣板建立新的excel檔。 
   * 更新excel的資料與第4點的分析功能使用同樣資料結果，由此類別繼承來共享函式，既可以確保結果一致，也不用到處修改散落各地的資料處理程式。
   * 對於要填入的資料如果有加入警報，如果資料本身超過警戒值，會在excel對該筆資料加入警告顏色。

### 3. 選股功能:
   * 主要介面包含excel樣板、所有內建好的選股條件、起始與回測日期，訊息欄(scrolled_text)，清除訊息、更新系統快取、離開、結束程式按鈕。
   * 選股策略以及回測系統皆為finlab提供的程式，並做小幅度微調。
   * 進入時，會帶入有預先儲存好的選股條件(前一次的啟動條件)，以及起始日期，可以根據自己的策略做簡單調整，最終在訊息欄會顯示在當下財報符合選股策略者。
   * 可將選股結果在指定路徑儲存成excel檔，選股結果也會存財報分析頁面的下拉選單以供選擇。
   * 執行回測則是以當下的選條件進行一段時間的結果，會顯示績效結果的matplotlib圖。
   * 執行結果、回測系統皆是使用異步完成，執行期間的步驟流程會顯示於訊息欄中。

### 4. 財報分析功能:
   * 介面主要包含可輸入股票id欄位，切換歷史搜尋/選股結果按鈕，分析月報、季報、金流分析等等。
   * 每項功能都會於介面按鈕左下方產生treeview表單，有些右下角會顯示一些matplotlib圖片，treeview會對一些數值危險的列上色示警。
   * 對於產生介面結果有做一些細部調整:
     1. 介面大小會根據顯示總元件結果做調整、
     2. 表單的欄位大小會根據最大內容調整、
     3. 表單有綁定滾元件固定總大小、
     4. 有些列有綁定事件用以顯示趨勢圖片。
   * 抓取資料的方式是，將需要的代號財報欄位一口氣抓出，再將資料整理過的結果儲存於類別變數中，以便當作快取使用，儲存的資料格式是index:(股票id, 時間), columns: (分類、細項)。
   * 抓取並清理資料的結果是與更新excel功能共用的，故可以減少重複工作，以及資料結果不一致的可能。
   * 程式採用同步執行，由於繪製treeview元件、繪圖、增加下拉選單選項都無法用異步來做，抓取資料及整理資料時間不會太長，故選用同步。
