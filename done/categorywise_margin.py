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

payload = "{\"query\":\"query suggi_GetSaleWiseProfit { suggi_getSaleWiseProfit_limited { _id customerDetails { name onboarding_date  village { name }  pincode { code } cust_type GSTIN iscouponapplicable address  { city street  pincode } phone customer_uid} storeDetails { name address {   pincode } vertical territory { name zone { name } } } userDetails { name address {   pincode } } invoiceno invoicedate product { soldqty servicecharge extradiscount sellingprice gst purchaseProductDetails {   name   category {     name   }   manufacturer {     name   }   sub_category {     name   } } lotDetails {  discount rate id sellingprice invoice { supplier_ref invoiceno createddate invoicedate } cnStockproduct subqty transportCharges otherCharges landingrate } supplierDetails { name } storeTargetPerProduct } payment { card cash upi } grosstotal storeCost } } \",\"variables\":{}}"
headers = {
    'Content-Type': 'application/json'
}
response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_getSaleWiseProfit_limited"]

df = pd.json_normalize(data)

start_date = "2024-07-29"
end_date = "2024-08-12"

def categorywise_margin(df, start_date, end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    month = start_date.month
    year = start_date.year

    start_date_week = start_date
    start_date_month = datetime(2024, month, 1)
    start_date_year = datetime(year, 4, 1)

    start_date_week_str = start_date_week.strftime("%Y-%m-%dT00:00:00Z")
    start_date_month_str = start_date_month.strftime("%Y-%m-%dT00:00:00Z")
    start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

    end_date = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")

    start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]

    actual_margins = []

    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for date_str in start_dates:
        start_date = pd.to_datetime(date_str)
        end_date = pd.to_datetime(end_date)

        filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
        
        df_exploded = filtered_df.explode('product')
        
        df_product = pd.json_normalize(df_exploded['product'])
        df_exploded = pd.json_normalize(filtered_df.to_dict(orient='records'))
        
        df_product['territory_name'] = df_exploded['storeDetails.territory.name']
        df_product['store_name'] = df_exploded['storeDetails.name']
        df_product['category_name'] = df_product['purchaseProductDetails.category.name']
        
        df_product = df_product[~df_product['territory_name'].isin(excluded_territories)]
        
        df_product['sellingprice_unit_xgst'] = df_product['sellingprice'] / (1 + (df_product['gst'] / 100))
        df_product['servicecharge_unit_xgst'] = df_product['servicecharge'] / (1 + (df_product['gst'] / 100))
        df_product['sale_value_xgst'] = df_product['soldqty'] * (df_product['sellingprice_unit_xgst'] + df_product['servicecharge_unit_xgst'])
        
        df_product['purchasingprice_unit'] = df_product['lotDetails.rate']
        df_product['lotDetails.discount'] = df_product['lotDetails.discount']
        df_product['purchase_value_xgst'] = df_product['soldqty'] * (df_product['purchasingprice_unit'] - df_product['lotDetails.discount'])
        
        df_product['actual_margin'] = df_product['sale_value_xgst'] - (
            df_product['purchase_value_xgst'] -
            (df_product['soldqty'] * (df_product['lotDetails.cnStockproduct'] / df_product['lotDetails.subqty'])) +
            (df_product['soldqty'] * (df_product['lotDetails.transportCharges'] / df_product['lotDetails.subqty'])) +
            (df_product['soldqty'] * (df_product['lotDetails.otherCharges'] / df_product['lotDetails.subqty']))
        )
        
        grouped_margins = df_product.groupby(['category_name'])['actual_margin'].sum().reset_index()
        grouped_margins.columns = ['Category', 'Total Actual Margin']

        actual_margins.append(grouped_margins)

        total_actual_margin = pd.concat(actual_margins).groupby('Category')['Total Actual Margin'].sum().reset_index()

        print("Total Actual Margin excluding specified territories:")
        print(total_actual_margin)

    # scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    # client = gspread.authorize(credentials)
    # spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    # worksheet = spreadsheet.get_worksheet(19)
    # cells = ['E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8']
    # for x,i in zip(cells, total_actual_margin):
    #   worksheet.update(values=[[i]], range_name=x)

categorywise_margin(df, start_date, end_date)

def categorywise_achieved(df,start_date,end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])

    start_date_str = start_date
    end_date_str = end_date

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    month = start_date.month
    year = start_date.year

    start_date_week = start_date
    start_date_month = datetime(2024, month, 1)
    start_date_year = datetime(year, 4, 1)

    start_date_week_str = start_date_week.strftime("%Y-%m-%d")
    start_date_month_str = start_date_month.strftime("%Y-%m-%d")
    start_date_year_str = start_date_year.strftime("%Y-%m-%d")

    end_date = (datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%d")

    start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]
    sales_values = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for start_date_str in start_dates:
        start_date = pd.to_datetime(start_date_str).tz_localize("UTC")
        end_date = pd.to_datetime(end_date_str).tz_localize("UTC")

        filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]

        df1 = filtered_df.explode('product')
        df2 = pd.json_normalize(df1)
        df_exploded = filtered_df.explode('product')
                
        df_product = pd.json_normalize(df_exploded['product'])
        df1['category_name'] = df_product['purchaseProductDetails.category.name']
        df1['territory_name'] = df1['storeDetails.territory.name']
        df1 = df1[~df1['territory_name'].isin(excluded_territories)]

        df1['sellingprice_unit'] = df1['product'].apply(lambda x: x['sellingprice'])
        df1['servicecharge_unit'] = df1['product'].apply(lambda x: x['servicecharge'])
        df1['soldqty'] = df1['product'].apply(lambda x: x['soldqty'])
        df1['gst%_unit'] = df1['product'].apply(lambda x: x['gst'])

        df1['sellingprice_unit_xgst'] = df1['sellingprice_unit'] / (1 + (df1['gst%_unit'] / 100))
        df1['servicecharge_unit_xgst'] = df1['servicecharge_unit'] / 1.18
        df1['sale_value_xgst'] = df1['soldqty'] * (df1['sellingprice_unit_xgst'] + df1['servicecharge_unit_xgst'])

        grouped_sales = df1.groupby('category_name')['sale_value_xgst'].sum().reset_index()
        grouped_sales.columns = ['Category', 'Total Sale Value (excl. GST)']

        sales_values.append(grouped_sales)

    for i, start_date_str in enumerate(start_dates):
        print(f"Sales for time frame starting {start_date_str}:")
        print(sales_values[i])
        print()

    # scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    # client = gspread.authorize(credentials)
    # spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    # worksheet = spreadsheet.get_worksheet(19)
    # cells = ['C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8',]
    # for x,i in zip(cells, sales_values):
    #   worksheet.update(values=[[i]], range_name=x)
    
categorywise_achieved(df,start_date,end_date)