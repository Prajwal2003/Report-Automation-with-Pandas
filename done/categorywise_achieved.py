import requests
import json
from datetime import datetime, timedelta
import pandas as pd

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

def categorywise_achieved(df, start_date, end_date):
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
    sales_values_dict = {}
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

        # Store the sales values in a dictionary
        sales_values_dict[start_date_str] = grouped_sales

        sales_values.append(grouped_sales)

    for i, start_date_str in enumerate(start_dates):
        print(f"Sales for time frame starting {start_date_str}:")
        print(sales_values[i])
        print()

    return sales_values_dict

# Call the function and store the sales values in a variable
sales_values_dict = categorywise_achieved(df, start_date, end_date)

# Example of how to access sales values for a specific time frame
print("Sales values for the week starting:", sales_values_dict["2024-07-29"])
