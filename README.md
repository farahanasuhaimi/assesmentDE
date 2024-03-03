# assesmentDE
## Question 1: Write a PySpark code
- [ ] Load both datasets into PySpark DataFrames, and clean and preprocess the data in each DataFrame.
- [ ] Extract the week number, quarter, and hour from the transaction date and add these as new columns to the sales DataFrame.
- [ ] Calculate Total sales by month, product, and the total sales amount for each customer.
- [ ] Identify the top 10 customers with the highest total sales amount.
- [ ] Generate a CSV for total sales by customer with email address and phone number.
- [ ] Export the results to a CSV file by month/year.

## Question 2: Write a Python script
- [x] Combine the data from all the JSON files into a single, standardized JSON format.
- [x] Handle any inconsistencies or missing data in the supplier files.
- [x] Detect and resolve duplicate entries.
- [ ] Store the consolidated data in a central data repository or database.

     Python script : [Code](./Q2.py)

## Question 3: Write a SQL code
- [x] Calculate the total sales amount for each customer for each month. The result should include the customer name, the month, and the total sales amount.

     SQL code 1: [Code](./Q3.1.sql)     
**The result name *monthlysales* is used as view table for 2nd task**
- [x] Calculate the 3-month moving average of each customer's sales. The result should include the customer name, the month, and the moving average of sales for each customer. The moving average should be calculated as an average of the sales for the current month and the two previous months.

     SQL code 2: [Code](./Q3.2.sql)     

## Question 4: Cloud-Based Data Pipeline for E-commerce Analytics
Based on given scenario, provide a detailed plan for setting up this cloud-based data pipeline. Specify:
-	The cloud provider you would choose for this project.
      - We have selected AWS services for most part of this pipeline.
-	The specific cloud tools or services you would use for data collection, processing, and storage.
      1. AWS Glue : to handle the various data formats and prepared for smoother and data cleaning and aggregation.
      2. AWS Lambda : to transform the data such as cleaning, aggregating, and preparing for the next step, i.e. data analysis. 
      3. Amazon S3 : To store processed and aggregated data. More like a pool (data lake) of data that can be retrieved in real-time efficiently such as for real-time data analytics.
      4. Amazon Redshift : Similar to S3 but a long-term storage that is optimized for larger or older databases hence the name data warehouse. 
      5. Microsoft Power BI or Tableau : Data analytics, real-time or scheduled depending on the needs, hourly, monthly, or yearly. 
-	Any potential challenges or considerations that need to be addressed.
      1. Scalability: with simple data ETL by AWS glue, larger and heavy traffics stream such as Khairul Aming's live Shopee required a better service, such as AWS Kinesis Data Streams service and Amazon EMR service.
      2. Fault tolerance: an efficient monitoring services of each data sources and load balancer may be needed with bigger scale of e-commerce company.
      3. Costing: AWS is know to be expensive with bigger load in certain services. Open source services such as Apache Iceberg, Compute Spark and Airflow Mage. If the company is starting small, it can pay for Amazon S3 for the cloud storage while utilising other open source toolkits. 
-	Please outline your plan, including the architecture and the tools or services you would utilize for each step of the data pipeline.
      ![pipeline](./Data%20Pipeline%20for%20E-commerce%20Analytics.png)
