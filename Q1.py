from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType
from pyspark.sql.functions import when, udf, weekofyear, quarter, hour, month, year, sum

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
customers_df_cl = customers_df_cl.withColumn('Phone_new', customers_df_cl['Phone'].cast(LongType()))

# 4. Clean and preprocess the sales data
sales_df_cl = sales_df.dropna()
sales_df_cl = sales_df_cl.dropDuplicates()

sales_df_cl = sales_df_cl.withColumn("Quantity", 
                                     when(sales_df_cl["Quantity"] < 0, 
                                          -sales_df_cl["Quantity"]).otherwise(sales_df_cl["Quantity"]))
sales_df_cl = sales_df_cl.filter(sales_df_cl['Quantity'].cast(IntegerType()).isNotNull())
# save the cleaned data to a new CSV file  
# customers_df_cl.write.csv("customer_data_Q1_cleaned.csv", header=True)
# sales_df_cl.write.csv("transaction_data_Q1_cleaned.csv", header=True)

# Extract the week number, quarter, and hour from the transaction date and add these as new columns to the sales DataFrame.
sales_df_cl = sales_df_cl.withColumn("week_number", weekofyear(sales_df_cl["TransactionDate"]))
sales_df_cl = sales_df_cl.withColumn("quarter", quarter(sales_df_cl["TransactionDate"]))
sales_df_cl = sales_df_cl.withColumn("hour", hour(sales_df_cl["TransactionDate"]))

# Calculate Total sales by month, product, and the total sales amount for each customer.
total_sales_by_month= sales_df_cl.groupBy(
    F.year("TransactionDate"), F.month("TransactionDate")).agg(F.sum(F.col("Price") * F.col("Quantity")).alias("TotalSales"))
total_sales_by_month = total_sales_by_month.withColumn("TotalSales", F.round(total_sales_by_month["TotalSales"], 2))
total_sales_by_month.show()

total_sales_by_product = sales_df_cl.groupBy("ProductCode").agg(F.sum(F.col("Price") * F.col("Quantity")).alias("TotalSales"))
total_sales_by_product = total_sales_by_product.withColumn("TotalSales", F.round(total_sales_by_product["TotalSales"], 2))
total_sales_by_product.show()

total_sales_by_customer = sales_df_cl.groupBy("CustomerID").agg(F.sum(F.col("Price") * F.col("Quantity")).alias("TotalSales"))
total_sales_by_customer = total_sales_by_customer.withColumn("TotalSales", F.round(total_sales_by_customer["TotalSales"], 2))
total_sales_by_customer.show()

# Identify the top 10 customers with the highest total sales amount.
top_10_customers = total_sales_by_customer.sort(F.desc("TotalSales")).limit(10)
top_10_customers.show()

# Generate a CSV for total sales by customer with email address and phone number.

# Export the results to a CSV file by month/year.