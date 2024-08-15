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

payload = "{\"query\":\"query suggi_GetSaleWiseProfit { suggi_getSaleWiseProfit_limited { _id customerDetails { name onboarding_date  village { name }  pincode { code } cust_type GSTIN iscouponapplicable address  { city street  pincode } phone customer_uid} storeDetails { name address {   pincode } vertical territory { name zone { name } } } userDetails { name address {   pincode } } invoiceno invoicedate product { soldqty servicecharge extradiscount sellingprice gst purchaseProductDetails {   name   category {     name   }   manufacturer {     name   }   sub_category {     name   } } lotDetails {  discount rate id sellingprice invoice { supplier_ref invoiceno createddate invoicedate } cnStockproduct subqty transportCharges otherCharges landingrate } supplierDetails { name } storeTargetPerProduct } payment { card cash upi } grosstotal storeCost } } \",\"variables\":{}}"

headers = {
    'Content-Type': 'application/json'
}

url = "http://13.126.125.132:4000"
response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_getSaleWiseProfit_limited"]
df = pd.DataFrame(data)

df = pd.json_normalize(data)

start_date = "2024-07-29"
end_date = "2024-08-12"

print(df.columns.to_list())

def storewise_achieved(df,start_date,end_date):
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
    print(dates for dates in start_dates)
    sales_values = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for start_date_str in start_dates:

        start_date = pd.to_datetime(start_date_str).tz_localize("UTC")
        end_date = pd.to_datetime(end_date_str).tz_localize("UTC")
        print(start_date)
        print(end_date)
        filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]

        df1 = filtered_df.explode('product')
        df1['territory_name'] = df1['storeDetails.territory.name']
        df1 = df1[~df1['territory_name'].isin(excluded_territories)]
        
        df1['sellingprice_unit'] = df1['product'].apply(lambda x: x['sellingprice'])
        df1['servicecharge_unit'] = df1['product'].apply(lambda x: x['servicecharge'])
        df1['soldqty'] = df1['product'].apply(lambda x: x['soldqty'])
        df1['gst%_unit'] = df1['product'].apply(lambda x: x['gst'])

        df1['sellingprice_unit_xgst'] = df1['sellingprice_unit'] / (1 + (df1['gst%_unit'] / 100))
        df1['servicecharge_unit_xgst'] = df1['servicecharge_unit'] / 1.18
        df1['sale_value_xgst'] = df1['soldqty'] * (df1['sellingprice_unit_xgst'] + df1['servicecharge_unit_xgst'])

        grouped_sales = df1.groupby('storeDetails.name')['sale_value_xgst'].sum().reset_index()
        grouped_sales.columns = ['Territory', 'Total Sale Value (excl. GST)']
        
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
    # worksheet = spreadsheet.get_worksheet(18)
    # cells = ['C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C30', 'C31', 'C32', 'C33', 'C34', 'C35', 'C36', 'C37', 'C38', 'C39', 'C40', 'C41', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12', 'F13', 'F14', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26', 'F27', 'F28', 'F30', 'F31', 'F32', 'F33', 'F34', 'F35', 'F36', 'F37', 'F38', 'F39', 'F40', 'F41', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9', 'I10', 'I11', 'I12', 'I13', 'I14', 'I16', 'I17', 'I18', 'I19', 'I20', 'I21', 'I22', 'I23', 'I24', 'I25', 'I26', 'I27', 'I28', 'I30', 'I31', 'I32', 'I33', 'I34', 'I35', 'I36', 'I37', 'I38', 'I39', 'I40', 'I41']
    # for x,i in zip(cells, sales_values):
    #   worksheet.update(values=[[i]], range_name=x)
    
storewise_achieved(df,start_date,end_date)