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
      - ![pipeline](./Data%20Pipeline%20for%20E-commerce%20Analytics.png)
-	The specific cloud tools or services you would use for data collection, processing, and storage.
      1. AWS Glue :
      2. AWS Lambda : 
      3. Amazon S3 :
      4. Amazon Redshift :
      5. Microsoft Power BI or Tableau : 
-	Any potential challenges or considerations that need to be addressed.
-	Please outline your plan, including the architecture and the tools or services you would utilize for each step of the data pipeline.

