import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

payload = {
    "query": """
    query Territory {
  suggi_getInventoryTer {
    storeDetails {
      territory {
        name
      }
      name
    }
    qty
    expirydate
    status
    rate
    subqty
    soldqty
    productDetails {
      category {
        name
      }
    }
    invoice {
      invoicedate
      invoiceno
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

data = response.json()["data"]["suggi_getInventoryTer"]

df = pd.json_normalize(data)

def inventory_territory(df):
    df['invoicedate'] = pd.to_datetime(df['invoice.invoicedate'],format='mixed')
    df['days_to_expiry'] = (pd.Timestamp.now(tz='UTC') - df['invoicedate']).dt.days
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    conditions = [
        (df['days_to_expiry'] > 0) & (df['days_to_expiry'] <= 45),
        (df['days_to_expiry'] > 45) & (df['days_to_expiry'] <= 90),
        (df['days_to_expiry'] > 90) & (df['days_to_expiry'] <= 135),
        (df['days_to_expiry'] > 135)
    ]

    categories = ['1-45 days', '46-90 days', '91-135 days', '135+ days']
    df['territory_name'] = df['storeDetails.territory.name']
    df = df[~df['territory_name'].isin(excluded_territories)]

    df['expiry_category'] = pd.cut(df['days_to_expiry'], bins=[1, 45, 90, 135, float('inf')], labels=categories)

    df['quantity'] = df['subqty'] - (df['soldqty'] - df['subqty'].where(df['status'] == "creditnote", 0))
    df['current_inventory_value'] = df['quantity'] * df['rate']
    grouped_df = df.groupby(['storeDetails.territory.name','expiry_category'])['current_inventory_value'].sum().reset_index()

    grouped_df.columns = ['Territory', 'Expiry Category', 'Purchase Value']
    purchase_values_list = grouped_df['Purchase Value'].tolist()
    print(grouped_df)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                    "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['B2', 'B3', 'B4', 'B5', 'C2', 'C3', 'C4', 'C5', 'D2', 'D3', 'D4', 'D5', 'E2', 'E3', 'E4', 'E5']
    for x,i in zip(cells, purchase_values_list):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)
        
inventory_territory(df)