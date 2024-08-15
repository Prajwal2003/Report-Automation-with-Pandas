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

def territorywise_target(df,start_date,end_date):

    start_date_str = datetime.strptime(start_date, "%Y-%m-%d")
    month = start_date.month
    year = start_date.year

    start_date_week = start_date_str
    start_date_month = datetime(2024, month, 1)
    start_date_year = datetime(year, 4, 1)

    start_date_week_str = start_date_week.strftime("%Y-%m-%dT00:00:00Z")
    start_date_month_str = start_date_month.strftime("%Y-%m-%dT00:00:00Z")
    start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

    end_date_str = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")

    start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]  

    revenue = []    
    for date_str in start_dates:    
        df['Date'] = pd.to_datetime(df['Date'])
        
        start_date = pd.to_datetime(date_str)
        end_date = pd.to_datetime(end_date_str)
        
        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        grouped_df = filtered_df.groupby('TM')
        for x,y in grouped_df:
          print(x)
          print(y)
          
        sum = (grouped_df['Daily_Target'].sum())
          
        revenu = []
        revenue.append(sum)
        print(type(revenue))

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
    # worksheet = spreadsheet.get_worksheet(17)
    # cells = ['B2', 'B3', 'B4', 'B5', 'F2', 'F3', 'F4', 'F5', 'J2', 'J3', 'J4', 'J5']
    # for x,i in zip(cells, revenu):
    #   worksheet.update(values=[[i]], range_name=x)
    
territorywise_target(df,start_date,end_date)