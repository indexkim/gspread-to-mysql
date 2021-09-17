import pandas as pd
from gssheet import GSSheets
from mydb import MySQLConnect

js = r'C:/Users/Jisoo/Desktop/gspread-to-mysql/json/gspread.json' #  GCP 프로젝트 서비스 계정 key
url = 'https://docs.google.com/spreadsheets/d/myspreadsheeturl1q2w3e4r/edit#gid=0' # 서비스 계정의 접근 권한을 부여한 spreadsheet의 url

gs = GSSheets(js, url) 
d = MySQLConnect('localhost', 'root', 'password', 'gspread-test') # host, user, password, db

def change_sheets(json_file_name, spreadsheet_url, sheet_num): # 전처리
    df = gs.open_sheets(json_file_name, spreadsheet_url, sheet_num)

    # Status Code의 앞자리만 표기 ex) 201 > 2
    df['Status Code'] = df['Status Code'].str[:1]

    # Original Url에서 Domain 추출한 Domain column 생성
    url_split = df['Original Url'].str.split('com/')
    df['Domain'] = url_split.str.get(0) + 'com/'

    # Month 를 Year, Month, Day, Date 로 분류하여 별도의 Column 생성 (Month의 dtype :datetime64[ns])
    df['Date'] = pd.to_datetime(df['Month']).dt.date.astype('str') # Name: Month, dtype: datetime64[ns] - astype('str')을 사용하지 않으면 dtype: object
    date_split = df['Date'].str.split('-')
    df['Year'] = date_split.str.get(0)
    df['Month'] = date_split.str.get(1)
    df['Day'] = date_split.str.get(2)

    # column 순서 정리
    df = df[[
        'Date', 'Year', 'Month', 'Day', 'Original Url', 'Domain',
        'Status Code'
    ]]
    return df

data_path = r'C:\Users\Jisoo\Desktop\gspread-to-mysql/gspread-test.xlsx'    # 원본 엑셀 데이터
data_before = pd.read_excel(data_path, sheet_name=None)

for i in range(len(data_before.keys())): # 원본 엑셀 시트 개수
    df = pd.read_excel(data_path, sheet_name=list(data_before.keys())[i])
    gs.upload_sheets(js, url, list(data_before.keys())[i], df)

#gs.delete_sheets(js, url, 0) # 시트가 1개 이상 존재해야 함 - 원본 파일에서 가져온 시트 개수보다 1개 많음 - 필요없는 시트 삭제
#gs.delete_sheets_by_name(js, url, '시트1') 

sheet_cnt = len(gs.access_sheets(js, url).worksheets()) # gspread 시트 개수
for i in range(sheet_cnt):
    data_after = change_sheets(js, url, i)
    d.db_upload(data_after, 'gspread-to-mysql-test1') 
