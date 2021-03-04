import mysql.connector
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

'''
Database Connection
'''

mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Douzi123!',
    database='premier_products',
    port=3306)

df_masters = pd.read_sql('SELECT * FROM listoforder_f', con=mydb)
df_incoming = pd.read_sql('SELECT * FROM sales_f', con=mydb)

#Output data
file_masters_dups = 'masters_dups.csv'
file_out = 'Walmart_US_all.xlsx'

'''
STEP 1: Left join to one table including all data, load to MySQL database
'''

# Uniqueness dimension: Extract Duplicate record from Master Dataframe into a new dataframe
df_dups = df_masters[df_masters.duplicated(keep='first')]
df_dups.to_csv(file_masters_dups)
df_masters.drop_duplicates(subset='OrderID', keep='first', inplace=True)

# Left join
df_join_left = pd.merge(df_masters, df_incoming, how='left', on=['OrderID'])
df_join_left.to_excel(file_out, index=False)

# Export to SQL
engine = create_engine('mysql+pymysql://root:Douzi123!@localhost/premier_products?charset=utf8')

try:
    df = pd.read_excel(file_out)
    df.to_sql('WalmartSalesAll',con=engine,if_exists='replace',index=False)
except Exception as e:
    print(e)


"""
STEP 2: Star Schema
Fact: Sales
Dimensions: Customer, Product, Ship, Time, SalesFact
Input data = Walmart_US_all
Output data = Customer, Product, Ship, Time, SalesFact in Database
            
"""

file_input = 'WalmartSalesAll.xlsx'

file_pre_processed = 'Pre-Processed Walmart Sales Lens Data.csv'
file_customer_output = 'Customer.xlsx'
file_product_output = 'Product.xlsx'
file_shipping_output = 'Shipping.xlsx'
file_time_output = 'Time.xlsx'
file_fact_output = 'Sales_Fact.xlsx'

df_input = df_join_left

# Pre processing
# Validity Quality Dimensions
df_input['Segment'] = df_input.Segment.replace(20141213, np.nan)
df_input['ShipMode'] = df_input.ShipMode.replace(110121, np.nan)
df_input['OrderDate'] = df_input.OrderDate.replace('Belgium', np.nan)
# Completeness Quality Dimensions, drop null value
df_input.dropna(axis='rows', inplace=True)
df_input.to_csv(file_pre_processed, encoding='utf-8', index=False)

df_pre_processed = pd.read_csv(file_pre_processed, header = 0)

# customer_output
df_customer = df_pre_processed.loc[ : , ['CustomerID', 'CustomerName', 'City', 'State', 'Region', 'Country'] ]
df_customer = df_customer.sort_values(by = ['CustomerID'], ascending = True, na_position = 'last').drop_duplicates(['CustomerID'],keep = 'first')
df_customer.to_excel(file_customer_output, index=False)
df_customer.to_sql('DSnowCustomer',con=engine,if_exists='replace',index=False)
# product_output
df_product = df_pre_processed.loc[ : , ['Product_ID', 'ProductName', 'SubCategory', 'Category', 'Segment'] ]
df_product = df_product.sort_values(by = ['Product_ID'], ascending = True, na_position = 'last').drop_duplicates(['Product_ID'],keep = 'first')
df_product.to_excel(file_product_output, index=False)
df_product.to_sql('DSnowProduct',con=engine,if_exists='replace',index=False)
# # shipping_output
df_shipping = df_pre_processed.loc[ : , ['ShipID', 'ShipMode'] ]
df_shipping = df_shipping.sort_values(by = ['ShipID'], ascending = True, na_position = 'last').drop_duplicates(['ShipID'],keep = 'first')
df_shipping.dropna(axis='rows', inplace=True)
df_shipping.to_excel(file_shipping_output, index=False)
df_shipping.to_sql('DSnowShip',con=engine,if_exists='replace',index=False)
# # time_output
df_time = df_pre_processed.loc[ : , ['OrderID', 'OrderDate', 'ShipDate', 'Date_MM/YY'] ]
df_time = df_time.sort_values(by = ['OrderID'], ascending = True, na_position = 'last').drop_duplicates(['OrderID'],keep = 'first')
df_time.to_excel(file_time_output, index=False)
df_time.to_sql('DSnowTime',con=engine,if_exists='replace',index=False)
## sales_output
df_sales_fact = df_pre_processed.loc[ : , ['OrderID', 'CustomerID', 'Product_ID', 'ShipID', 'Sales', 'Profit', 'Quantity', 'Discount'] ]
df_sales_fact = df_sales_fact.sort_values(by = ['OrderID'], ascending = True, na_position = 'last').drop_duplicates(['OrderID'],keep = 'first')

# Sales, Profit cleaning
df_sales_fact['Sales'] = df_sales_fact.Sales.str.replace('US\$', '', regex=True)
df_sales_fact['Sales'] = df_sales_fact.Sales.str.replace(',','')
df_sales_fact['Profit'] = df_sales_fact.Profit.str.replace('US\$', '', regex=True)
df_sales_fact['Profit'] = df_sales_fact.Profit.str.replace(',','')
df_sales_fact.to_excel(file_fact_output, index=False)
df_sales_fact.to_sql('FSnowSales',con=engine,if_exists='replace',index=False)