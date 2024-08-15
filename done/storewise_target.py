import csv
import glob
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import itertools
import requests
import json
from datetime import datetime, timedelta

url = "http://13.126.125.132:4000"

payload = "{\"query\":\"query Suggi_StoreTarget {\\n  suggi_StoreTarget {\\n    Store\\n    TM\\n    Category\\n    Month\\n    Year\\n    Target\\n    Date\\n    Daily_Target\\n  }\\n}\",\"variables\":{}}"
headers = {
  'Content-Type': 'application/json'
}

response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_StoreTarget"]
df = pd.DataFrame(data)
print(df.columns.to_list())

start_date = "2024-07-29"
end_date = "2024-08-04"

def storewise_target(df,start_date,end_date):
    start_date_str = start_date
    end_date_str = end_date

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    month = start_date.month
    year = start_date.year

    start_date_week = start_date
    start_date_month = datetime(2024, month, 1)
    start_date_year = datetime(year, 4, 1)

    start_date_week_str = start_date_week.strftime("%Y-%m-%dT00:00:00Z")
    start_date_month_str = start_date_month.strftime("%Y-%m-%dT00:00:00Z")
    start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

    end_date = (datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")

    start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]  

    revenue = []    
    for date_str in start_dates:    
        df['Date'] = pd.to_datetime(df['Date'])
        
        start_date = pd.to_datetime(date_str)
        end_date = pd.to_datetime(end_date)
        
        
        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        grouped_df = filtered_df.groupby('Store')
          
        sum = (grouped_df['Daily_Target'].sum())
          
        revenu = []
        revenue.append(sum)

        for x in revenue:
          print(type(x))
          x = x.astype(float)
          x = x/100000
          revenu.append(x)
        print(revenu)

    # scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    # client = gspread.authorize(credentials)
    # spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    # worksheet = spreadsheet.get_worksheet(18)
    # cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'B13', 'B14', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21', 'B22', 'B23', 'B24', 'B25', 'B26', 'B27', 'B28', 'B30', 'B31', 'B32', 'B33', 'B34', 'B35', 'B36', 'B37', 'B38', 'B39', 'B40', 'B41', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'H10', 'H11', 'H12', 'H13', 'H14', 'H16', 'H17', 'H18', 'H19', 'H20', 'H21', 'H22', 'H23', 'H24', 'H25', 'H26', 'H27', 'H28', 'H30', 'H31', 'H32', 'H33', 'H34', 'H35', 'H36', 'H37', 'H38', 'H39', 'H40', 'H41', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'K10', 'K11', 'K12', 'K13', 'K14', 'K16', 'K17', 'K18', 'K19', 'K20', 'K21', 'K22', 'K23', 'K24', 'K25', 'K26', 'K27', 'K28', 'K30', 'K31', 'K32', 'K33', 'K34', 'K35', 'K36', 'K37', 'K38', 'K39', 'K40', 'K41']
    # for x,i in zip(cells, sales_values):
    #   worksheet.update(values=[[i]], range_name=x)
    
storewise_target(df,start_date,end_date)