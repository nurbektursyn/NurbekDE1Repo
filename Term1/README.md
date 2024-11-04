# Coffee Bean Sales Raw Dataset Project

[The Coffee Bean Sales Dataset](https://www.kaggle.com/datasets/saadharoon27/coffee-bean-sales-raw-dataset), retireved from Kaggle, offers a comprehensive view into the coffee industry, capturing key data on customer demographics, order transactions, and a variety of coffee products. 
The dataset will be used to analyze sales trends and consumer behavior. This README serves as a guide to setting up, running, and understanding the project.

## Project Documentation

### 1. Data Structure

The dataset came in the raw .xlsx format with 3 sheets (orders, customers, producsts) in it. See ***'Coffee.xlsx'*** for more details. For simplicity, I have divided these 3 sheets into 3 separate .csv files.

There are:
1. **Coffee_Customers.csv**
  - Contains customer details:

  - Customer ID (string): Unique identifier for each customer.
  - Customer Name (string): Full name of the customer.
  - Email (string): Customer's email address.
  - Phone Number (string): Contact number.
  - Address Line 1 (string): Primary address of the customer.
  - City (string): City of residence.
  - Country (string): Country of residence.
  - Postcode (string): Postal code.
  - Loyalty Card (string): Indicates if the customer has a loyalty card ("Yes"/"No").
  
2. **Coffee_Orders.csv**
  - Contains order details:

  - Order ID (string): Unique identifier for each order.
  - Order Date (date): Date the order was placed.
  - Customer ID (string): Unique identifier for each customer.
  - Product ID (string): Unique identifier for each coffee product.
  - Quantity (integer): Number of units ordered.
3. **Coffee_Products.csv**
  - Contains product details:
  
  - Product ID (string): Unique identifier for each coffee product.
  - Coffee Type (string): Type of coffee (e.g., “Ara”).
  - Roast Type (string): Roast level of the coffee (e.g., Light, Medium).
  - Size (float): Weight/volume of the product in grams or liters.
  - Unit Price (float): Price per unit of the product.
  - Price per 100g (float): Standardized price per 100 grams for comparison.
  - Profit (float): Profit earned per unit sold.

### 2. Relationships Between Tables

- ***Customer-Order Relationship***: Customer ID in Coffee_Orders.csv links to Customer ID in Coffee_Customers.csv.
- ***Order-Product Relationship***: Product ID in Coffee_Orders.csv links to Product ID in Coffee_Products.csv.

## 2. Reproducibility Instructions

1. Download the repository or Install Manually as a ZIP File.
   - ```gh repo clone nurbektursyn/NurbekDE1Repo```
2. Create a Schema
   - Open MySQL Workbench and create a new schema named coffee:
     
```CREATE SCHEMA coffee;```
  - Refresh the schema list to see the newly created coffee schema.
3. Import Tables
  - In the coffee schema, right-click **Tables** and select **Table Data Import Wizard**. Use this wizard to upload the following CSV files:
    - Coffee_Orders.csv
    - Coffee_Products.csv
Follow the wizard’s prompts to map columns as needed.
4. Run SQL Scripts
  - Coffee_Customers.sql: Run this script to create and populate the Coffee_Customers table.
  - coffee_script.sql: Run this script to see the analysis and insights.

*Note: Due to importing issues (likely due to data types), directly importing Coffee_Customers.csv using wizard did not work. Instead, had to create a script using [CSV to SQL converter](https://www.convertcsv.com/csv-to-sql.htm).*
