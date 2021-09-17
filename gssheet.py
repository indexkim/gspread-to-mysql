import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from gspread_dataframe import set_with_dataframe

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

class GSSheets:
    def __init__(self, json_file_name, spreadsheet_url):
        self.scope = DEFAULT_SCOPES
        self.json_file_name = json_file_name
        self.spreadsheet_url = spreadsheet_url

    def access_sheets(self, json_file_name, spreadsheet_url):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.json_file_name, self.scope)
        gc = gspread.authorize(credentials)
        sh = gc.open_by_url(self.spreadsheet_url)
        return sh

    def open_sheets(self, json_file_name, spreadsheet_url, sheet_num):
        sh = self.access_sheets(json_file_name, spreadsheet_url)
        worksheet_list = sh.worksheets()
        worksheet = sh.get_worksheet(sheet_num)
        df = worksheet.get_all_values()
        df = pd.DataFrame(df, columns=df[0])
        df = df.reindex(df.index.drop(0))
        print('시트 개수:', len(worksheet_list), '시트 번호:', sheet_num)
        return df

    def upload_sheets(self, json_file_name, spreadsheet_url, wsheet_name, data):
        sh = self.access_sheets(json_file_name, spreadsheet_url)
        worksheet = sh.add_worksheet(title=wsheet_name, rows='100', cols='20')
        set_with_dataframe(worksheet, data)
        df = worksheet.get_all_values()
        df = pd.DataFrame(df, columns=df[0])
        df = df.reindex(df.index.drop(0))
        print(f'Your data has been uploaded to {wsheet_name} successfully.')
        return df

    def delete_sheets(self, json_file_name, spreadsheet_url, sheet_num):
        sh = self.access_sheets(json_file_name, spreadsheet_url)
        worksheet = sh.get_worksheet(sheet_num)
        worksheet = sh.del_worksheet(worksheet)
        print(f'{sheet_num}th sheet has been deleted successfully.')

    def delete_sheets_by_name(self, json_file_name, spreadsheet_url, wsheet_name):
        sh = self.access_sheets(json_file_name, spreadsheet_url)
        worksheet = sh.worksheet(wsheet_name)
        worksheet = sh.del_worksheet(worksheet)
        print(f'Sheet name {wsheet_name} has been deleted successfully.')