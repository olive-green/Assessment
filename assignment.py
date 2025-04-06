# -*- coding: utf-8 -*-
"""Assignment

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1--NPhgmkc_dHZyk8dE3TeqwrbeWxg5yD
"""

import requests
import pandas as pd
import gzip
from io import BytesIO

#Download the .csv.gz file
url = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
response = requests.get(url)

#Check if the request was successful
if response.status_code == 200:
    #Read the gzip-compressed content into a pandas DataFrame
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        df = pd.read_csv(gz)
    print("File downloaded and data loaded into DataFrame successfully!")
    print(df.head())
else:
    print("Failed to download the file. Status code:", response.status_code)

print(df.columns)

print(df.describe())

print(df.dtypes)

print(df.count())

print(df.isnull().sum())

import numpy as np
#Standardize column names for SQL compatibility
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(' ', '_')
    .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
)

#Remove duplicate rows if any
df.drop_duplicates(inplace=True)

#Replace empty strings with NaN
df.replace('', np.nan, inplace=True)

#Convert boolean column
df['is_free_shipping'] = df['is_free_shipping'].astype(bool)

#Convert numeric columns
numeric_columns = [
    'platform_commission_rate', 'promotion_price', 'current_price', 'product_commission_rate',
    'bonus_commission_rate', 'discount_percentage', 'price', 'rating_avg_value',
    'number_of_reviews', 'seller_rating'
]
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

#Handle missing values
df['brand_name'] = df['brand_name'].fillna('Unknown')
df['image_url_5']=df['image_url_5'].fillna('Unknown')
df['image_url_2']=df['image_url_2'].fillna('Unknown')
df['image_url_4']=df['image_url_4'].fillna('Unknown')
df['venture_category3_name_en'] = df['venture_category3_name_en'].fillna('Unknown')
df['description'] = df['description'].fillna('')
df['number_of_reviews'] = df['number_of_reviews'].fillna(0)
df['seller_rating'] = df['seller_rating'].fillna(df['seller_rating'].median())

#Standardize text columns
text_columns = df.select_dtypes(include='object').columns
df[text_columns] = df[text_columns].apply(lambda x: x.str.strip())

#Optional – drop columns with >50% missing values
missing_threshold = 0.5
cols_to_drop = df.columns[df.isnull().mean() > missing_threshold]
df.drop(columns=cols_to_drop, inplace=True)

print(df.shape)

print(df.isnull().sum())

import sqlite3
from sqlalchemy import create_engine, text

# === CONFIG ===
db_name = "tyroo_products.db"
table_name = "product_data"
chunk_size = 50000

# === Create SQLAlchemy engine ===
engine = create_engine(f"sqlite:///{db_name}")


create_table_sql = """
CREATE TABLE IF NOT EXISTS product_data (
    platform_commission_rate     REAL,
    venture_category3_name_en    TEXT,
    product_small_img            TEXT,
    deeplink                     TEXT,
    availability                 TEXT,
    image_url_5                  TEXT,
    number_of_reviews            INTEGER,
    is_free_shipping             BOOLEAN,
    promotion_price              REAL,
    venture_category2_name_en    TEXT,
    current_price                REAL,
    product_medium_img           TEXT,
    venture_category1_name_en    TEXT,
    brand_name                   TEXT,
    image_url_4                  TEXT,
    description                  TEXT,
    seller_url                   TEXT,
    product_commission_rate      REAL,
    product_name                 TEXT,
    sku_id                       TEXT,
    seller_rating                REAL,
    bonus_commission_rate        REAL,
    business_type                TEXT,
    business_area                TEXT,
    image_url_2                  TEXT,
    discount_percentage          REAL,
    seller_name                  TEXT,
    product_url                  TEXT,
    product_id                   BIGINT,
    venture_category_name_local  TEXT,
    rating_avg_value             REAL,
    product_big_img              TEXT,
    image_url_3                  TEXT,
    price                        REAL
);
"""

#Create the table using the schema
with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    print("Table schema created.")

#Insert DataFrame into the database in chunks ===

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"Inserted chunk {i//chunk_size + 1}")

print("\nAll data inserted successfully into database:", db_name)