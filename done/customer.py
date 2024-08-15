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

payload = {
    "query": """
    query CustomerDetails {
      suggi_getSaleWiseProfit {
        customerDetails {
          name
        }
        storeDetails {
          territory {
            name
          }
        }
      }
    }
    """,
    "variables": {}
}

headers = {
    'Content-Type': 'application/json'
}

url = "http://13.126.125.132:4000"
response = requests.post(url, headers=headers, json=payload)

if response.status_code != 200:
    print(f"Failed to fetch data: {response.status_code}")
    print(response.text)
    exit()

data = response.json()["data"]["suggi_getSaleWiseProfit"]
df = pd.json_normalize(data)

def customer_engagment(df):

    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    df = df[~df['storeDetails.territory.name'].isin(excluded_territories)]

    distinct_customer_count = df['customerDetails.name'].nunique()
    grouped_df = df.groupby('storeDetails.territory.name')['customerDetails.name'].nunique().reset_index()
    grouped_df.columns = ['Territory', 'Distinct Customer Count']
    cust_count = grouped_df['Distinct Customer Count'].tolist()

    print(f"Total Distinct Customers: {distinct_customer_count}")
    print(grouped_df)
    
    # scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    # client = gspread.authorize(credentials)
    # spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    # worksheet = spreadsheet.get_worksheet(23)
    # cells = ['C2', 'C3', 'C4', 'C5']
    # for x,i in zip(cells, cust_count):
    #   worksheet.update(values=[[i]], range_name=x)
    
customer_engagment(df)