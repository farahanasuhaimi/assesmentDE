from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("CSV Cleaning") \
    .getOrCreate()
# Load both datasets into PySpark DataFrames, and clean and preprocess the data in each DataFrame.
customers_df = spark.read.csv("customer_data_Q1.csv", header=True, inferSchema=True)
sales_df     = spark.read.csv("transaction_data_Q1.csv", header=True, inferSchema=True)
# Extract the week number, quarter, and hour from the transaction date and add these as new columns to the sales DataFrame.
customers_df.show()
# Calculate Total sales by month, product, and the total sales amount for each customer.

# Identify the top 10 customers with the highest total sales amount.

# Generate a CSV for total sales by customer with email address and phone number.

# Export the results to a CSV file by month/year.