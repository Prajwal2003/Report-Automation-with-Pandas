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
import time

url = "http://13.126.125.132:4000"

payload = "{\"query\":\"query suggi_GetSaleWiseProfit { suggi_getSaleWiseProfit_limited { _id customerDetails { name onboarding_date  village { name }  pincode { code } cust_type GSTIN iscouponapplicable address  { city street  pincode } phone customer_uid} storeDetails { name address {   pincode } vertical territory { name zone { name } } } userDetails { name address {   pincode } } invoiceno invoicedate product { soldqty servicecharge extradiscount sellingprice gst purchaseProductDetails {   name   category {     name   }   manufacturer {     name   }   sub_category {     name   } } lotDetails {  discount rate id sellingprice invoice { supplier_ref invoiceno createddate invoicedate } cnStockproduct subqty transportCharges otherCharges landingrate } supplierDetails { name } storeTargetPerProduct } payment { card cash upi } grosstotal storeCost } } \",\"variables\":{}}"
headers = {
    'Content-Type': 'application/json'
}
response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_getSaleWiseProfit_limited"]
df1 = pd.json_normalize(data)

payload = "{\"query\":\"query Suggi_StoreTarget {\\n  suggi_StoreTarget {\\n    Store\\n    TM\\n    Category\\n    Month\\n    Year\\n    Target\\n    Date\\n    Daily_Target\\n  }\\n}\",\"variables\":{}}"
headers = {
  'Content-Type': 'application/json'
}

response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_StoreTarget"]
df2 = pd.DataFrame(data)

payload = {"query": "query Territory { suggi_getInventoryTer { storeDetails { territory { name } } qty expirydate status rate subqty soldqty productDetails { category { name } } invoice { invoicedate } } }", "variables": {}}
headers = {
    'Content-Type': 'application/json'
}

response = requests.post(url, headers=headers, json=payload)
data = response.json()["data"]["suggi_getInventoryTer"]
df3 = pd.json_normalize(data)

def get_mondays(date=None):
    if date is None:
        date = datetime.now().date()
    
    weekday = date.weekday()
    days_to_last_monday = (weekday - 0) % 7
    last_monday = date - timedelta(days=days_to_last_monday)
    previous_monday = last_monday - timedelta(days=7)
    
    return last_monday, previous_monday

end_date, start_date = get_mondays()

month = start_date.month
year = start_date.year

start_date_week = start_date
start_date_month = datetime(2024, month, 1)
start_date_year = datetime(year, 4, 1)

start_date_week_str = start_date_week.strftime("%Y-%m-%dT00:00:00Z")
start_date_month_str = start_date_month.strftime("%Y-%m-%dT00:00:00Z")
start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

end_date = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")
start_dates = [start_date_week_str, start_date_month_str, start_date_year_str]

def Suggi_first(df1,df2,start_dates, end_date):
    
    def achieved_revenue(df, start_dates, end_date):
        revenue = []
        for date_str in start_dates:
            df['invoicedate'] = pd.to_datetime(df['invoicedate'])
            
            start_date = pd.to_datetime(date_str)
            end_date = pd.to_datetime(end_date)
            
            filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
            df1 = filtered_df.explode(['product'])
            df1['sellingprice_unit'] = df1['product'].apply(lambda x: x['sellingprice'])
            df1['servicecharge_unit'] = df1['product'].apply(lambda x: x['servicecharge'])
            df1['soldqty'] = df1['product'].apply(lambda x: x['soldqty'])

            df1['gst%_unit'] = df1['product'].apply(lambda x: x['gst'])
            df1['sellingprice_unit_xgst'] = df1['sellingprice_unit'] / (1 + (df1['gst%_unit'] / 100))
            df1['servicecharge_unit_xgst'] = df1['servicecharge_unit'] / 1.18
            df1['sale_value_xgst'] = df1['soldqty'] * (df1['sellingprice_unit_xgst'] + df1['servicecharge_unit_xgst'])
            sum = (df1['sale_value_xgst'].sum())
            revenue.append(sum)
            
        revenue_float = [round((float(x))/100000, 2) for x in revenue]    
        return(revenue_float)

    def target_revenue(df, start_dates, end_date):
        revenue = []    
        for date_str in start_dates:    
            df['Date'] = pd.to_datetime(df['Date'])
            start_date = pd.to_datetime(date_str)
            end_date = pd.to_datetime(end_date)
            
            filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
            sum = (filtered_df['Daily_Target'].sum())
            revenue.append(sum)
            
        revenue_float = [round((float(x))/100000, 2) for x in revenue]
        return(revenue_float)

    def actual_margin(df, start_date_str, end_date_str):

        df['invoicedate'] = pd.to_datetime(df['invoicedate'])
        actual_margins = []

        for date_str in start_date_str:
            start_date = pd.to_datetime(date_str)
            end_date = pd.to_datetime(end_date_str)

            filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
            
            df_exploded = filtered_df.explode('product')
            
            df_product = pd.json_normalize(df_exploded['product'])
            
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
            actual_margins.append(df_product['actual_margin'].sum())

        actual_margins_float = [round((float(x))/100000, 2) for x in actual_margins]
        percentages = [str(num) + "%" for num in actual_margins_float]
        return(percentages)
  
    def achieved_purchase(df, start_dates, end_date):
        df['invoicedate'] = pd.to_datetime(df['invoicedate'])
        achieve_purchase = []

        for date_str in start_dates:
            start_date = pd.to_datetime(date_str)
            end_date = pd.to_datetime(end_date)

            filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
            df_exploded = filtered_df.explode('product')
            df_product = pd.json_normalize(df_exploded['product'])
            
            df_product['achived_purchase'] = df_product['soldqty'] * (df_product['lotDetails.rate'])
            achieve_purchase.append(df_product['achived_purchase'].sum())
      
        achieve_purchase_float = [round((float(x))/100000, 2) for x in achieve_purchase]
        return(achieve_purchase_float)
    
    ach_rev = achieved_revenue(df1,start_dates, end_date)
    tar_rev = target_revenue(df2, start_dates, end_date)
    act_mar = actual_margin(df1,start_dates, end_date)
    ach_pur = achieved_purchase(df1,start_dates, end_date)

    print(ach_rev)
    print(tar_rev)
    print(act_mar)
    print(ach_pur)

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(creds)
    spreadsheet_name = 'Weekly Report Siri Suggi(1)'
    sheet = client.open(spreadsheet_name)
    worksheet = sheet.get_worksheet(17)

    cells = ['C2', 'F2', 'I2']
    for x,i in zip(cells, ach_rev):
            worksheet.update(values=[[i]], range_name=x)

    cells = ['B2', 'E2', 'H2']
    for x,i in zip(cells, tar_rev):
            worksheet.update(values=[[i]], range_name=x)

    cells = ['C5', 'F5', 'I5']    
    for x,i in zip(cells, act_mar):
            worksheet.update(values=[[i]], range_name=x)
            
    cells = ['C3', 'F3', 'I3']    
    for x,i in zip(cells, ach_pur):
            worksheet.update(values=[[i]], range_name=x)

def categorywise_margin(df, start_dates, end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])

    actual_margins = []
    margin = []
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
        margin.extend(total_actual_margin['Total Actual Margin'].tolist())

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8']
    for x,i in zip(cells, margin):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)

def categorywise_achieved(df,start_dates,end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    sales_values = []
    sales = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for start_date_str in start_dates:
        start_date = pd.to_datetime(start_date_str)
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
        else:
            start_date = start_date.tz_convert("UTC")
        
        end_date_str = pd.to_datetime(end_date)
        if end_date_str.tzinfo is None:
            end_date_str = end_date_str.tz_localize("UTC")
        else:
            end_date_str = end_date_str.tz_convert("UTC")

        filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date_str)]

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
        sales.extend(grouped_sales['Total Sale Value (excl. GST)'].tolist())
        sales_values.append(grouped_sales)

    for i, start_date_str in enumerate(start_dates):
        print(f"Sales for time frame starting {start_date_str}:")
        print(sales_values[i])
        print()
    for i,j in enumerate(sales):
        sales[i]=round(float(j/100000),2)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8',]
    for x, i in zip(cells, sales):
        worksheet.update(values=[[i]], range_name=x)

def categorywise_target(df,start_dates,end_date):
    
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    revenue_tolist = [] 
    for date_str in start_dates:    
        df['Date'] = pd.to_datetime(df['Date'])
        
        start_date = pd.to_datetime(date_str)
        end_date = pd.to_datetime(end_date)
        df1 = df[~df['TM'].isin(excluded_territories)]
        filtered_df = df1[(df1['Date'] >= start_date) & (df1['Date'] <= end_date)]
        revenue = []
        grouped_df = filtered_df.groupby('Category')
        sum = (grouped_df['Daily_Target'].sum())
        revenue.append(sum)
        revenue_tolist.extend(sum.tolist())

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'J2', 'J3', 'J4', 'J5', 'J6', 'J7', 'J8']
    for x,i in zip(cells, revenue_tolist):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)

def storewise_achieved(df,start_dates,end_date_str):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    print(dates for dates in start_dates)
    sales_values = []
    sales = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for start_date_str in start_dates:

        start_date = pd.to_datetime(start_date_str)
        print(start_date)
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
        else:
            start_date = start_date.tz_convert("UTC")
        
        end_date = pd.to_datetime(end_date_str)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")
        else:
            end_date = end_date.tz_convert("UTC")
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
        sales.extend(grouped_sales['Total Sale Value (excl. GST)'].tolist())

    for i, start_date_str in enumerate(start_dates):
        print(f"Sales for time frame starting {start_date_str}:")
        print(sales_values[i])
        print()

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(19)
    cells = ['C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C30', 'C31', 'C32', 'C33', 'C34', 'C35', 'C36', 'C37', 'C38', 'C39', 'C40', 'C41', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12', 'F13', 'F14', 'F16', 'F17', 'F18', 'F19', 'F20', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26', 'F27', 'F28', 'F30', 'F31', 'F32', 'F33', 'F34', 'F35', 'F36', 'F37', 'F38', 'F39', 'F40', 'F41', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9', 'I10', 'I11', 'I12', 'I13', 'I14', 'I16', 'I17', 'I18', 'I19', 'I20', 'I21', 'I22', 'I23', 'I24', 'I25', 'I26', 'I27', 'I28', 'I30', 'I31', 'I32', 'I33', 'I34', 'I35', 'I36', 'I37', 'I38', 'I39', 'I40', 'I41']
    for x,i in zip(cells, sales):
        i = round((i/100000),2)
        time.sleep(1)
        worksheet.update(values=[[i]], range_name=x)

def storewise_margin(df, start_dates, end_dates):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    start_date_str = start_dates[2]
    actual_margins = []
    margin = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    start_date = pd.to_datetime(start_date_str)
    if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
    else:
            start_date = start_date.tz_convert("UTC")
        
    end_date = pd.to_datetime(end_dates)
    if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")
    else:
            end_date = end_date.tz_convert("UTC")

    filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
        
    df_exploded = filtered_df.explode('product')
        
    df_product = pd.json_normalize(df_exploded['product'])
    df_exploded = pd.json_normalize(filtered_df.to_dict(orient='records'))
        
    df_product['territory_name'] = df_exploded['storeDetails.territory.name']
    df_product['store_name'] = df_exploded['storeDetails.name']
        
    df_product = df_product[~df_product['territory_name'].isin(excluded_territories)]
        
    df_product['sellingprice_unit_xgst'] = df_product['sellingprice'] / (1 + (df_product['gst'] / 100))
    df_product['servicecharge_unit_xgst'] = df_product['servicecharge'] / (1 + (df_product['gst'] / 100))
    df_product['sale_value_xgst'] = df_product['soldqty'] * (df_product['sellingprice_unit_xgst'] + df_product['servicecharge_unit_xgst'])
        
    df_product['purchasingprice_unit'] = df_product['lotDetails.rate']
    df_product['lotDetails.discount'] = df_product['lotDetails.discount']
    df_product['purchase_value_xgst'] = df_product['soldqty'] * (df_product['purchasingprice_unit'] - df_product['lotDetails.discount'])
        
    df_product['actual_margin'] = df_product['sale_value_xgst'] - (df_product['purchase_value_xgst'] -
        (df_product['soldqty'] * (df_product['lotDetails.cnStockproduct'] / df_product['lotDetails.subqty'])) +
        (df_product['soldqty'] * (df_product['lotDetails.transportCharges'] / df_product['lotDetails.subqty'])) +
        (df_product['soldqty'] * (df_product['lotDetails.otherCharges'] / df_product['lotDetails.subqty'])))
        
    grouped_margins = df_product.groupby(['store_name'])['actual_margin'].sum().reset_index()
    grouped_margins.columns = ['Store','Total Actual Margin']
    margin.extend(grouped_margins['Total Actual Margin'].tolist())
    actual_margins.append(grouped_margins)

    print(f"Actual margins for time frame starting :")
    print(actual_margins)
    print()
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(19)
    cells = ['K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'K10', 'K11', 'K12', 'K13', 'K14', 'K16', 'K17', 'K18', 'K19', 'K20', 'K21', 'K22', 'K23', 'K24', 'K25', 'K26', 'K27', 'K28', 'K30', 'K31', 'K32', 'K33', 'K34', 'K35', 'K36', 'K37', 'K38', 'K39', 'K40', 'K41']
    for x,i in zip(cells, margin):
      i = round((i/100000),2)  
      time.sleep(1)
      worksheet.update(values=[[i]], range_name=x)

def storewise_target(df,start_dates,end_date):
    
    revenue = []
    revenue_tolist = []  
    for date_str in start_dates:    
        df['Date'] = pd.to_datetime(df['Date'])
        
        start_date = pd.to_datetime(date_str)
        end_date = pd.to_datetime(end_date)
        
        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        grouped_df = filtered_df.groupby('Store')
        sum = (grouped_df['Daily_Target'].sum())
        revenue.append(sum)
        revenue_tolist.extend(sum.tolist())
        print(revenue)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(19)
    cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'B13', 'B14', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21', 'B22', 'B23', 'B24', 'B25', 'B26', 'B27', 'B28', 'B30', 'B31', 'B32', 'B33', 'B34', 'B35', 'B36', 'B37', 'B38', 'B39', 'B40', 'B41', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 'E16', 'E17', 'E18', 'E19', 'E20', 'E21', 'E22', 'E23', 'E24', 'E25', 'E26', 'E27', 'E28', 'E30', 'E31', 'E32', 'E33', 'E34', 'E35', 'E36', 'E37', 'E38', 'E39', 'E40', 'E41', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'H10', 'H11', 'H12', 'H13', 'H14', 'H16', 'H17', 'H18', 'H19', 'H20', 'H21', 'H22', 'H23', 'H24', 'H25', 'H26', 'H27', 'H28', 'H30', 'H31', 'H32', 'H33', 'H34', 'H35', 'H36', 'H37', 'H38', 'H39', 'H40', 'H41']
    for x,i in zip(cells, revenue_tolist):
      i = round((i/100000),2)
      time.sleep(1)
      worksheet.update(values=[[i]], range_name=x)
    
def territorywise_margin(df, start_dates, end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    actual_margins = []
    margin = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for date_str in start_dates:
        start_date = pd.to_datetime(date_str)
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
        else:
            start_date = start_date.tz_convert("UTC")
        
        end_date = pd.to_datetime(end_date)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")
        else:
            end_date = end_date.tz_convert("UTC")

        filtered_df = df[(df['invoicedate'] >= start_date) & (df['invoicedate'] <= end_date)]
        
        df_exploded = filtered_df.explode('product')
        
        df_product = pd.json_normalize(df_exploded['product'])
        df_exploded = pd.json_normalize(filtered_df.to_dict(orient='records'))
        
        df_product['territory_name'] = df_exploded['storeDetails.territory.name']
        
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
        
        grouped_margins = df_product.groupby('territory_name')['actual_margin'].sum().reset_index()
        grouped_margins.columns = ['Territory', 'Total Actual Margin']
        margin.extend(grouped_margins['Total Actual Margin'].tolist())
        actual_margins.append(grouped_margins)

    for i, date_str in enumerate(start_dates):
        print(f"Actual margins for time frame starting {date_str}:")
        print(actual_margins[i])
        print()
        
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(18)
    cells = ['E2', 'E3', 'E4', 'E5', 'I2', 'I3', 'I4', 'I5', 'M2', 'M3', 'M4', 'M5']
    for x,i in zip(cells, margin):
      i = round((i/100000),2)  
      worksheet.update(values=[[i]], range_name=x)

def territorywise_achieved(df,start_dates,end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])
    sales_values = []
    sales = []
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]

    for start_date_str in start_dates:

        start_date = pd.to_datetime(start_date_str)
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
        else:
            start_date = start_date.tz_convert("UTC")
        
        end_date = pd.to_datetime(end_date)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")
        else:
            end_date = end_date.tz_convert("UTC")
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

        grouped_sales = df1.groupby('storeDetails.territory.name')['sale_value_xgst'].sum().reset_index()
        grouped_sales.columns = ['Territory', 'Total Sale Value (excl. GST)']
        sales.extend(grouped_sales['Total Sale Value (excl. GST)'].tolist())
        sales_values.append(grouped_sales)

    for i, start_date_str in enumerate(start_dates):
        print(f"Sales for time frame starting {start_date_str}:")
        print(sales_values[i])
        print()

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(18)
    cells = ['C2', 'C3', 'C4', 'C5', 'G2', 'G3', 'G4', 'G5', 'K2', 'K3', 'K4', 'K5']
    for x,i in zip(cells, sales):
      i = round((i/100000),2)  
      worksheet.update(values=[[i]], range_name=x)

def territorywise_target(df,start_dates,end_dates):

    revenue = []
    revenue_tolist = []  
    for date_str in start_dates:    
        df['Date'] = pd.to_datetime(df['Date'])
        
        start_date = pd.to_datetime(date_str)
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")
        else:
            start_date = start_date.tz_convert("UTC")
        
        end_date = pd.to_datetime(end_dates)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")
        else:
            end_date = end_date.tz_convert("UTC")
        
        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        grouped_df = filtered_df.groupby('TM')
        sum = (grouped_df['Daily_Target'].sum())
        revenue.append(sum)
        revenue_tolist.extend(sum.tolist())
    print(revenue)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(18)
    cells = ['B2', 'B3', 'B4', 'B5', 'F2', 'F3', 'F4', 'F5', 'J2', 'J3', 'J4', 'J5']
    for x,i in zip(cells, revenue_tolist):
      i = round((i/100000),2)  
      worksheet.update(values=[[i]], range_name=x)

def inventory_category(df):
    df['invoicedate'] = pd.to_datetime(df['invoice.invoicedate'],format='mixed')
    df['days_to_expiry'] = (pd.Timestamp.now(tz='UTC') - df['invoicedate']).dt.days
    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    conditions = [
        (df['days_to_expiry'] >= 1) & (df['days_to_expiry'] <= 45),
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
    grouped_df = df.groupby(['productDetails.category.name','expiry_category'])['current_inventory_value'].sum().reset_index()

    grouped_df.columns = ['Category', 'Expiry Category', 'Purchase Value']
    purchase_values_list = grouped_df['Purchase Value'].tolist()

    print(grouped_df)
    print(purchase_values_list)


    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                    "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7', 'E8']
    for x,i in zip(cells, purchase_values_list):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)

def expired_category(df):
    df['expirydate'] = pd.to_datetime(df['expirydate'], utc=True)
    df['invoicedate'] = pd.to_datetime(df['invoice.invoicedate'], utc=True)

    df['expiry_clock'] = (df['expirydate'] - pd.Timestamp.now(tz='UTC')).dt.days

    df['expiry_filter'] = df.apply(lambda row: 'Expired' if row['expiry_clock'] <= 0 else 'Not Expired', axis=1)

    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    df = df[~df['storeDetails.territory.name'].isin(excluded_territories)]

    df['returned_qty'] = df.apply(lambda row: row['subqty'] if row['status'] == 'creditnote' else 0, axis=1)
    df['quantity'] = df['subqty'] - (df['soldqty'] - df['returned_qty'])
    df['current_inventory_value'] = df['quantity'] * df['rate']

    grouped_df = df[df['expiry_filter'] == 'Expired'].groupby('productDetails.category.name')['current_inventory_value'].sum().reset_index()

    grouped_df.columns = ['Category Name', 'Total Expired Purchase Value']
    purchase_values_list = grouped_df['Total Expired Purchase Value'].tolist()
    print(grouped_df)

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                    "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(20)
    cells = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8']
    for x,i in zip(cells, purchase_values_list):
        i = round((i/100000),2)
        worksheet.update(values=[[i]], range_name=x)

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

def customer_engagment(df):

    excluded_territories = ["RJ RSP TERRITORY", "MH RSP TERRITORY", "MP RSP TERRITORY", "KA RSP TERRITORY", "HO"]
    df = df[~df['storeDetails.territory.name'].isin(excluded_territories)]

    distinct_customer_count = df['customerDetails.name'].nunique()
    grouped_df = df.groupby('storeDetails.territory.name')['customerDetails.name'].nunique().reset_index()
    grouped_df.columns = ['Territory', 'Distinct Customer Count']
    cust_count = grouped_df['Distinct Customer Count'].tolist()

    print(f"Total Distinct Customers: {distinct_customer_count}")
    print(grouped_df)
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    worksheet = spreadsheet.get_worksheet(24)
    cells = ['C2', 'C3', 'C4', 'C5']
    for x,i in zip(cells, cust_count):  
      worksheet.update(values=[[i]], range_name=x)
    
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

# Suggi_first( df1, df2, start_dates, end_date)
# time.sleep(10)
# categorywise_margin(df1, start_dates, end_date)
# time.sleep(10)
# categorywise_achieved(df1,start_dates,end_date)
# time.sleep(10)
# categorywise_target(df2,start_dates,end_date)
# time.sleep(20)
# storewise_achieved(df1,start_dates,end_date)
# time.sleep(10)
# storewise_margin(df1,start_dates,end_date)
# time.sleep(10)
# storewise_target(df2,start_dates,end_date)
# time.sleep(10)
# territorywise_margin(df1, start_dates, end_date)
# territorywise_achieved(df1,start_dates,end_date)
# territorywise_target(df2,start_dates,end_date)
# time.sleep(10)
inventory_category(df3)
time.sleep(10)
expired_category(df3)
inventory_territory(df3)
time.sleep(10)
expired_store(df3)
time.sleep(10)
# customer_engagment(df1)