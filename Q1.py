from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import when,udf

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("CSV Cleaning") \
    .getOrCreate()
# Load both datasets into PySpark DataFrames, and clean and preprocess the data in each DataFrame.
customers_df = spark.read.csv("customer_data_Q1.csv", header=True, inferSchema=True)
sales_df     = spark.read.csv("transaction_data_Q1.csv", header=True, inferSchema=True)

# 1. Clean and preprocess the customer data
customers_df_cl = customers_df.dropna()
customers_df_cl = customers_df_cl.dropDuplicates()

# 2. Clean email addresses
def add_email_col(email):
    if '@example.com' in email:
        username = email.split('@')[0]
        return username + '@example.com'
    elif 'example.com' in email:
        username = email.split('example.com')[0]
        return username + '@example.com'
    else:
        return username
    
add_email_col_udf = udf(add_email_col, StringType())
customers_df_cl = customers_df_cl.withColumn('email', add_email_col_udf('email'))

# 3. Clean phone numbers

# Extract the week number, quarter, and hour from the transaction date and add these as new columns to the sales DataFrame.

# Calculate Total sales by month, product, and the total sales amount for each customer.

# Identify the top 10 customers with the highest total sales amount.

# Generate a CSV for total sales by customer with email address and phone number.

# Export the results to a CSV file by month/year.