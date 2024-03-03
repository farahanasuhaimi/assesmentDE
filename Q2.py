import json
import pandas as pd
import os

directory = '.'  # The directory where the JSON files are stored
all_files = os.listdir(directory)
data_list = []

# Handle any inconsistencies or missing data in the supplier files.
def standardize_data(product_data):
    """
    Standardize the data from the supplier files into a single, consistent format.
    """
    standardized_data = {
        'product_id': product_data.get('product_id', product_data.get('SKU', '')),
        'product_name': product_data.get('product_name', 'Unknown Product'),
        'price': product_data.get('price', 0),
        'description': product_data.get('description', 'No description available'),
        'manufacturer': product_data.get('manufacturer', 'Unknown Manufacturer'),
        'stock_quantity': product_data.get('stock_quantity', product_data.get('inventory', 0))
    }
    return standardized_data

# Get all json files with the standard names
json_files = [file for file in all_files if file.startswith('product_') and file.endswith('.json')]

for filename in json_files:
    with open(filename) as f:
        data_list.append(json.load(f))

# Combine the data from all the JSON files into a single, standardized JSON format.
standardized_data = [standardize_data(product_data) for product_data in data_list]
df = pd.DataFrame(standardized_data)

# Detect and resolve duplicate entries.
consolidated_df = df.groupby('product_id').agg({
    'product_name': 'first', 
    'price': 'first',
    'description': 'first',
    'manufacturer': 'first',
    'stock_quantity': 'sum'  # Sum the stock_quantity values
}).reset_index()

# Save the consolidated data to a CSV file
consolidated_df.to_csv('consolidated_product_data.csv', index=False)

# Store the consolidated data in a central data repository or database.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ConsolidatedDataStorage").getOrCreate()
spark_df = spark.createDataFrame(consolidated_df)

# spark_df.write.format("jdbc") \
#     .option("url", "jdbc:mysql://hostname:port/database_name") \
#     .option("dbtable", "products") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .mode("overwrite") \
#     .save()
# spark.stop()