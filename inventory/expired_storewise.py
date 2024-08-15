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

data = response.json()["data"]["suggi_getInventoryTer"]
df = pd.json_normalize(data)

def expired_store(df):
    df['expirydate'] = pd.to_datetime(df['expirydate'], utc=True)
    df['invoicedate'] = pd.to_datetime(df['invoice.invoicedate'], utc=True)

    df['expiry_clock'] = (df['expirydate'] - pd.Timestamp.now(tz='UTC')).dt.days

    df['expiry_filter'] = df.apply(lambda row: 'Expired' if row['expiry_clock'] <= 0 else 'Not Expired', axis=1)

    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    df = df[~df['storeDetails.territory.name'].isin(excluded_territories)]

    df['returned_qty'] = df.apply(lambda row: row['subqty'] if row['status'] == 'creditnote' else 0, axis=1)
    df['quantity'] = df['subqty'] - (df['soldqty'] - df['returned_qty'])

    df['current_inventory_value'] = df['quantity'] * df['rate']

    grouped_df = df[df['expiry_filter'] == 'Expired'].groupby('storeDetails.name')['current_inventory_value'].sum().reset_index()

    grouped_df.columns = ['Store Name', 'Total Expired Purchase Value']
    purchase_values_list = grouped_df['Total Expired Purchase Value'].tolist()
    print(grouped_df)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                    "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21', 'B22', 'B23', 'B24']
    for x,i in zip(cells, purchase_values_list):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)
        
expired_store(df)