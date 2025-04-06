# Tyroo Data Cleaning & SQL Ingestion Pipeline

This project handles the end-to-end pipeline for:
- Downloading a large gzipped CSV file from a public URL
- Cleaning and transforming the data using pandas
- Creating a SQL table schema
- Inserting the cleaned data into a local SQLite database in chunks

# Data Source

The data is downloaded from the following URL:
https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz

# Tech Stack

- Python 3.8+
- pandas
- SQLAlchemy
- SQLite (can be replaced with PostgreSQL/MySQL)

# Setup Instructions

1. Clone this repository

   git clone https://github.com/olive-green/Assessment
   cd Assessment

2. Create a virtual environment (optional but recommended)

   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

3. Install the required dependencies

   pip install -r requirements.txt

   Note: requirements.txt should include: pandas, sqlalchemy, requests

#  Data Cleaning & Transformation

The cleaning script performs:
- Column name normalization (snake_case)
- Conversion of numeric and boolean columns
- Missing value handling with strategies (e.g., fillna or median)
- Text stripping and standardization
- Dropping columns with >50% missing values
- Type conversions for SQL compatibility

# SQL Table Schema

A SQLite table is created using the following schema:

CREATE TABLE product_data (
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
    product_id                   BIGINT PRIMARY KEY,
    venture_category_name_local  TEXT,
    rating_avg_value             REAL,
    product_big_img              TEXT,
    image_url_3                  TEXT,
    price                        REAL
);

# Data Insertion

The cleaned DataFrame is inserted into SQLite using chunk-wise insertion for performance:

chunk_size = 50000
df.to_sql(name="product_data", con=engine, if_exists='append', index=False, chunksize=chunk_size)

 # Output

- Cleaned data stored in: tyroo_products.db
- SQL Table: product_data
- Log: Prints status of each chunk inserted

# Next Steps

- Connect to PostgreSQL/MySQL (via sqlalchemy)
- Add indexing on product_id, brand_name, etc. for query performance
- Build dashboard or API to interact with cleaned dataset

# Contributions

Feel free to fork, contribute, or suggest improvements. PRs are welcome!


