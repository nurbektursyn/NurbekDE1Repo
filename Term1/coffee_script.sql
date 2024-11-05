-------------------------------------------------------------------------------------------------------------------------------------------
-- Author       Nurbek Bektursyn
-- Created      27.10.2024
-- Purpose      The Coffee Bean Sales Dataset, retireved from Kaggle, offers a comprehensive view into the coffee industry, 
-- 				capturing key data on customer demographics, order transactions, and a variety of coffee products. 
--              The dataset will be used to analyze sales trends and consumer behavior. 

-------------------------------------------------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS coffee;

USE coffee;

-- ANALYTICS

/* 
This project aims to understand sales trends and analyze customers' purchasing behaviour. In terms of sales trends, I am interested to
explore seasonality of sales, demand patters of different products, as well as coffee types that generate the highest revenue. In terms of
customers' purchasing behaviour, I want to gain insights on customer loyalty, demographics, and preferences.

Analytical Metrics:
- Monthly Sales: To measure monthly revenue trends.
- Avg. Order Value: To understand customer spending patterns.
- Distinct Customer Count by Region: To analyze geographical spread of customers.
- Product Category Sales: To identify high-demand coffee products.
- Customer Lifetime Value (CLV): To assess customer value over time.
- Loyalty Card Revenue Impact: To determine spending differences by loyalty status.

ETL Process
1. Source Data: Extract from primary tables.
2. Transform: Aggregate, join, and calculate metrics.
3. Load: Store metrics in `ProductSales` data mart.
4. Scheduling: Automate ETL refresh monthly.

Data Mart Structure
- `ProductSales` Mart: Monthly sales, category revenue.
*/

-- ANALYTICAL LAYER

-- CREATE A DENORMALIZED TABLE

DROP TABLE IF EXISTS coffee_sales; 
CREATE TABLE coffee_sales AS 
SELECT 
   o.`Order ID`,
   o.`Order Date`,
   o. Quantity,
   
   c.`Customer ID`,
   c.`Customer Name`,
   c.`Address Line 1` AS `Address`,
   c.City,
   c.Country,
   c.Postcode,
   c.`Loyalty Card`,
   
   p.`Product ID`,
   p.`Coffee Type`,
   p.`Roast Type`,
   p.Size,
   p.`Unit Price`,
   p.`Price per 100g`,
   p.Profit
   
FROM 
	coffee_orders o
INNER JOIN 
   coffee_customers c ON o.`Customer ID` = c.`Customer ID`
INNER JOIN 
   coffee_products p ON o.`Product ID` = p.`Product ID`;

SELECT * FROM coffee_sales;

SELECT DATE_FORMAT(`Order Date`, '%Y-%m') AS Month, --  Total Sales and Average Order Value by Month
	ROUND(SUM(Quantity * `Unit Price`), 2) AS Total_Sales,
	ROUND(AVG(Quantity * `Unit Price`), 2) AS Avg_Order_Value,
	CASE
		WHEN SUM(Quantity * `Unit Price`) >= 1260 THEN 'High'
		WHEN SUM(Quantity * `Unit Price`) BETWEEN 720 AND 1260 THEN 'Moderate'
		ELSE 'Low'
	END AS Sales_Category
FROM coffee_sales
GROUP BY Month
ORDER BY Total_Sales DESC;

-- February 2020 was the most profitable month, with total sales of 1,798.34. The highest average order value occurred in June 2020.
-- Thresholds values were used to categorize sales as "High," "Moderate," or "Low" based on the maximum (1798.34) and minimum (233.24)
-- total sales values. Values above 70% of the maximum were categorized as "High," and values below 40% of the maximum were categorized as "Low". Everything in between were
-- categorized as "Moderate".

SELECT 
    Country,
    COUNT(DISTINCT `Customer ID`) AS Distinct_Customer_Count
FROM coffee_sales
GROUP BY Country
ORDER BY Distinct_Customer_Count DESC;

-- Most Customers (~78%) are from the United States. 

SELECT 
	`Coffee Type`, 
	ROUND(SUM(Quantity * `Unit Price`), 2) AS Total_Sales,
	ROUND(AVG(Quantity * `Unit Price`), 2) AS Avg_Order_Value
FROM coffee_sales
GROUP BY `Coffee Type`
ORDER BY Total_Sales DESC;

-- Customers seem to prefer buying mostly Excelsa (Exc) and Liberica (Lib), 
-- as indicated by their higher total sales compared to Arabica (Ara) and Robusta (Rob).

-- Let's now see Customer Lifetime Value (CLV). CLV is the total anticipated revenue a company foresees from its average customer throughout their 
-- entire relationship. It's calculated as (Average Value of Sale) × (Average Number of Transactions) × (Average Customer Lifespan)

SELECT 
    ROUND(AVG(Total_Sales / Purchase_Count), 2) AS Avg_Value_of_Sale,
    ROUND(AVG(Purchase_Count), 2) AS Avg_Number_of_Transactions,
    ROUND(AVG(Customer_Lifetime_Months), 2) AS Avg_Customer_Lifespan,
    ROUND(AVG(Total_Sales / Purchase_Count) * AVG(Purchase_Count) * AVG(Customer_Lifetime_Months), 2) AS Customer_Lifetime_Value
FROM (
    SELECT 
        `Customer ID`,
        SUM(Quantity * `Unit Price`) AS Total_Sales,
        COUNT(`Order ID`) AS Purchase_Count,
        TIMESTAMPDIFF(MONTH, MIN(`Order Date`), MAX(`Order Date`)) + 1 AS Customer_Lifetime_Months
    FROM 
        coffee_sales
	GROUP BY `Customer ID`)
AS CustomerMetrics;

-- Most customers make only one or two purchases, typically within a day. 
-- Thus, the overall lifetime revenue per customer is relatively low ($73.61). This means that the company should work on 
-- customer retention and repeat purchase rates. 
    
-- Loyalty Card Revenue Impact
SELECT 
    `Loyalty Card` AS Loyalty_Status,
    COUNT(DISTINCT `Customer ID`) AS Customer_Count,
    ROUND(SUM(Quantity * `Unit Price`), 2) AS Total_Sales,
    ROUND(SUM(Quantity * `Unit Price`) / COUNT(DISTINCT `Customer ID`), 2) AS Avg_Revenue_Per_Customer
FROM 
	coffee_sales
GROUP BY Loyalty_Status;

-- Customers without a loyalty card bring in more revenue than those with a loyalty card, meaning the loyalty program is not effectively
-- utilized. 

-- Now let's combine what we've had so far into a stored procedure. 
SELECT * FROM coffee_sales;

-- ETL PIPLINE
DROP PROCEDURE IF EXISTS GetCoffeeSales;

DELIMITER $$

CREATE PROCEDURE GetCoffeeSales (
    IN startDate DATE,
    IN endDate DATE,
    IN country_name VARCHAR(255)
)
BEGIN
    -- Extract
    SELECT 
        `Customer ID`,
        `Customer Name`,
        `Coffee Type`,
        `Loyalty Card`,
        `Country`,
        `City`,
        -- Transform        
        SUM(Quantity) AS Total_Quantity,                 -- Total quantity of coffee sold
        SUM(Quantity * `Unit Price`) AS Total_Sales,     -- Calculated revenue
        COUNT(`Order Date`) AS Visits,               -- Total number of visits per customer
        -- Eligibility for a free cup: 1 if eligible, 0 if not
        CASE 
            WHEN MAX(`Loyalty Card`) = 'Yes' AND SUM(Quantity) > 4 THEN 1
            ELSE 0
        END AS Free_Cup_Eligibility
        
    FROM coffee_sales
    WHERE `Order Date` BETWEEN startDate AND endDate
    AND Country = country_name
    GROUP BY `Customer ID`, `Customer Name`, `Coffee Type`, `Loyalty Card`, `Country`, `City`
    ORDER BY Total_Sales DESC;
END $$

DELIMITER ;

-- Choose any date (YYYY-MM-DD) between 2019-01-02 and 2022-08-19;
-- Choose any country: 'United States', 'Ireland', 'United Kingdom';

CALL GetCoffeeSales('2019-04-12', '2021-05-12', 'United States');

DELIMITER $$

-- Create a schedule that will show updated sales report every month. 

CREATE EVENT Monthly_CoffeeSales_Report
ON SCHEDULE EVERY 1 MONTH
STARTS '2024-11-01 00:00:00'
DO
BEGIN
    DECLARE startDate DATE;
    DECLARE endDate DATE;

    -- Set the date range for the current month
    SET startDate = DATE_FORMAT(CURRENT_DATE, '%Y-%m-01');
    SET endDate = LAST_DAY(CURRENT_DATE);

    CALL GetCoffeeSales(startDate, endDate, 'United States');
END $$

DELIMITER ;

-- Turn "ON" if you want the abovementioned event to run 
SHOW VARIABLES LIKE 'event_scheduler';
SET GLOBAL event_scheduler = OFF;

-- DATA MART 1 (Product Sales)

DROP TABLE IF EXISTS mv_product_sales;
CREATE TABLE mv_product_sales (
    coffee_type VARCHAR(50),
    order_date DATE,
    total_quantity_sold INT,
    total_sales_amount DECIMAL(10, 2),
    avg_order_value DECIMAL(10, 2)
);

INSERT INTO mv_product_sales
SELECT `Coffee Type`, 
	`Order Date`,
    SUM(Quantity),
	ROUND(SUM(Quantity * `Unit Price`), 2),
	ROUND(SUM(Quantity * `Unit Price`) / SUM(Quantity), 2)
FROM coffee_sales
GROUP BY `Coffee Type`, `Order Date`
ORDER BY `Order Date` DESC; 

ALTER TABLE mv_product_sales
ADD UNIQUE INDEX unique_coffee_date (coffee_type, order_date);

SELECT * FROM mv_product_sales;

-- Trigger for inserting an entry
DROP TRIGGER IF EXISTS coffee_sales_ins;
DELIMITER $$

CREATE TRIGGER coffee_sales_ins
AFTER INSERT ON coffee_sales
FOR EACH ROW
BEGIN
    DECLARE current_total_quantity INT DEFAULT 0;
    DECLARE current_total_sales DECIMAL(10, 2) DEFAULT 0.0;
    DECLARE current_avg_order_value DECIMAL(10,2) DEFAULT 0.0;
    DECLARE new_total_quantity INT;
    DECLARE new_total_sales DECIMAL(10, 2);
    DECLARE new_avg_order_value DECIMAL(10, 2);

    -- Check if an entry exists in mv_product_sales
    SELECT total_quantity_sold, total_sales_amount, avg_order_value
    INTO current_total_quantity, current_total_sales, current_avg_order_value
    FROM mv_product_sales
    WHERE coffee_type = NEW.`Coffee Type` AND order_date = NEW.`Order Date`
    LIMIT 1;

    -- Calculate new totals
    SET new_total_quantity = current_total_quantity + NEW.Quantity;
    SET new_total_sales = current_total_sales + (NEW.Quantity * NEW.`Unit Price`);
    SET new_avg_order_value = ROUND(new_total_sales / new_total_quantity, 2);

    -- Update if the row exists, otherwise insert a new row
    INSERT INTO mv_product_sales (coffee_type, order_date, total_quantity_sold, total_sales_amount, avg_order_value)
    VALUES (NEW.`Coffee Type`, NEW.`Order Date`, new_total_quantity, new_total_sales, new_avg_order_value)
    ON DUPLICATE KEY UPDATE
        total_quantity_sold = new_total_quantity,
        total_sales_amount = new_total_sales,
        avg_order_value = new_avg_order_value;
END $$
DELIMITER ;

-- Trigger for Customer ID generation

DROP TRIGGER IF EXISTS generate_customer_id;
DELIMITER $$
CREATE TRIGGER generate_customer_id
BEFORE INSERT ON coffee_sales
FOR EACH ROW
BEGIN
    IF NEW.`Customer ID` IS NULL THEN
        SET NEW.`Customer ID` = CONCAT(
            LPAD(FLOOR(RAND() * 100000), 5, '0'), '-', 
            LPAD(FLOOR(RAND() * 100000), 5, '0'), '-',
            CHAR(FLOOR(RAND() * 26) + 65),  
            CHAR(FLOOR(RAND() * 26) + 65)
        );
    END IF;
END $$

DELIMITER ;

SELECT COUNT(*) FROM coffee_sales;

-- Check
INSERT INTO coffee_sales VALUES (
    'ABC-12345-678', '2024-11-01', 4, NULL, 'John Doe', '123 Elm Street', 
    'Los Angeles', 'United States', '90045', 'No', 'E-L-2.5', 'Exc', 
    'L', 2.5, 25.875, 1.3662, 3.75705
);


SELECT * FROM coffee_sales ORDER BY `Order Date` DESC;
SELECT * FROM mv_product_sales ORDER BY order_date DESC;

INSERT INTO coffee_sales VALUES (
    'DEF-12345-678', '2024-11-01', 2, NULL, 'Aby Doe', '123 Alm Street', 
    'Los Angeles', 'United States', '90045', 'No', 'E-L-2.5', 'Exc', 
    'L', 2.5, 25.875, 1.3662, 3.75705
);

SELECT * FROM coffee_sales ORDER BY `Order Date` DESC;
SELECT * FROM mv_product_sales ORDER BY order_date DESC;

-- Trigger for deleting an entry

DROP TRIGGER IF EXISTS coffee_sales_del;
DELIMITER $$

CREATE TRIGGER coffee_sales_del
AFTER DELETE ON coffee_sales
FOR EACH ROW
BEGIN
    DECLARE current_total_quantity INT DEFAULT 0;
    DECLARE current_total_sales DECIMAL(10, 2) DEFAULT 0.0;
    DECLARE current_avg_order_value DECIMAL(10,2) DEFAULT 0.0;
    DECLARE new_total_quantity INT;
    DECLARE new_total_sales DECIMAL(10, 2);
    DECLARE new_avg_order_value DECIMAL(10, 2);

    -- Check if an entry exists in mv_product_sales for the deleted row's coffee_type and order_date
    SELECT total_quantity_sold, total_sales_amount, avg_order_value
    INTO current_total_quantity, current_total_sales, current_avg_order_value
    FROM mv_product_sales
    WHERE coffee_type = OLD.`Coffee Type` AND order_date = OLD.`Order Date`
    LIMIT 1;

    -- Calculate new totals after deleting the row
    SET new_total_quantity = current_total_quantity - OLD.Quantity;
    SET new_total_sales = current_total_sales - (OLD.Quantity * OLD.`Unit Price`);

    -- Check if the new total quantity sold is greater than zero
    IF new_total_quantity > 0 THEN
        -- Calculate the new average order value
        SET new_avg_order_value = ROUND(new_total_sales / new_total_quantity, 2);

        -- Update the row in mv_product_sales with the new totals
        UPDATE mv_product_sales
        SET total_quantity_sold = new_total_quantity,
            total_sales_amount = new_total_sales,
            avg_order_value = new_avg_order_value
        WHERE coffee_type = OLD.`Coffee Type` AND order_date = OLD.`Order Date`;
    ELSE
        -- If total quantity is zero or less, delete the entry from mv_product_sales
        DELETE FROM mv_product_sales
        WHERE coffee_type = OLD.`Coffee Type` AND order_date = OLD.`Order Date`;
    END IF;

END $$
DELIMITER ;

-- Check
-- DELETE FROM coffee_sales WHERE `Order ID` = 'ABC-12345-678';
DELETE FROM coffee_sales WHERE `Order ID` = 'DEF-12345-678';

SELECT * FROM coffee_sales ORDER BY `Order Date` DESC;
SELECT * FROM mv_product_sales ORDER BY order_date DESC;
   
