# Developer - Afrin Rose
# Project   - Python Assessment 1
# Summary   - This will fetch all the rows from a source CSV file and then replace null/ special characters
#             and remove duplicate rows from source and load the cleaned data to Target SQL server table.

import pandas as pd
import pyodbc

# Import CSV from source folder(raw data)
data = pd.read_csv(r'C:\SourceFile\products.csv')

# replace null or empty cell in product_name column to have "no product name"
data["product_name"].fillna("No product name", inplace=True)

# remove special character from a column
cleanproduct_name = data.product_name.str.replace('[@#&$%+-/*]', '')

# reassign the new column(cleanproduct_name) to  dataframe real column(product_name)
data = data.assign(product_name=cleanproduct_name)

# sort the primary key column, it will be easier to find the duplicates
data.sort_values('product_id', inplace=True)

# find and remove duplicates
data.drop_duplicates(subset='product_id',
                     keep='first', inplace=True)

# assign the cleaned data to dataframe
df = pd.DataFrame(data)

# Connect to Target system (SQL Server)
conn = pyodbc.connect('Driver={SQL SERVER};'
                      'Server=DESKTOP-NLNS7S5;'
                      'Database=Python;'
                      'Trusted_Connection=yes;'
                      )
cursor = conn.cursor()

for row in df.itertuples():
    cursor.execute('''
                INSERT INTO products (product_id, product_name, price)
                VALUES (?,?,?)
                ''',
                   row.product_id,
                   row.product_name,
                   row.price)
    conn.commit()
