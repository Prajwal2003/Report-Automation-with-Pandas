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

payload = "{\"query\":\"query suggi_GetSaleWiseProfit { suggi_getSaleWiseProfit { _id customerDetails { name onboarding_date  village { name }  pincode { code } cust_type GSTIN iscouponapplicable address  { city street  pincode } phone customer_uid} storeDetails { name address {   pincode } vertical territory { name zone { name } } } userDetails { name address {   pincode } } invoiceno invoicedate product { soldqty servicecharge extradiscount sellingprice gst purchaseProductDetails {   name   category {     name   }   manufacturer {     name   }   sub_category {     name   } } lotDetails {  discount rate id sellingprice invoice { supplier_ref invoiceno createddate invoicedate } cnStockproduct subqty transportCharges otherCharges landingrate } supplierDetails { name } storeTargetPerProduct } payment { card cash upi } grosstotal storeCost } } \",\"variables\":{}}"
headers = {
    'Content-Type': 'application/json'
}
response = requests.post(url, headers=headers, data=payload)
data = response.json()["data"]["suggi_getSaleWiseProfit"]

df = pd.json_normalize(data)

start_date = "2024-07-29"
end_date = "2024-08-12"

def storewise_margin(df, start_date, end_date):
    df['invoicedate'] = pd.to_datetime(df['invoicedate'])

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    month = start_date.month
    year = start_date.year

    start_date_year = datetime(year, 4, 1)

    start_date_year_str = start_date_year.strftime("%Y-%m-%dT00:00:00Z")

    end_date = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")

    start_dates = [start_date_year_str]

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
        df_product['territory_name'] = df_exploded['storeDetails.territory.name']
        df_product['store_name'] = df_exploded['storeDetails.name']
        
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
        
        grouped_margins = df_product.groupby(['store_name'])['actual_margin'].sum().reset_index()
        grouped_margins.columns = ['Store','Total Actual Margin']

        actual_margins.append(grouped_margins)

    for i, date_str in enumerate(start_dates):
        print(f"Actual margins for time frame starting {date_str}:")
        print(actual_margins[i])
        print()
    
    # scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    # client = gspread.authorize(credentials)
    # spreadsheet = client.open("Weekly Report Siri Suggi(1)")
    # worksheet = spreadsheet.get_worksheet(18)
    # cells = ['K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'K10', 'K11', 'K12', 'K13', 'K14', 'K16', 'K17', 'K18', 'K19', 'K20', 'K21', 'K22', 'K23', 'K24', 'K25', 'K26', 'K27', 'K28', 'K30', 'K31', 'K32', 'K33', 'K34', 'K35', 'K36', 'K37', 'K38', 'K39', 'K40', 'K41']
    # for x,i in zip(cells, sales_values):
    #   worksheet.update(values=[[i]], range_name=x)

storewise_margin(df)