create database sqlmini_project;
use sqlmini_project;

#1. Join all the tables and create a new table called combined_table.
#(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)
select * from market_fact;
select * from cust_dimen;
select * from orders_dimen;
select * from prod_dimen;
select * from shipping_dimen;

CREATE TABLE combined_table AS
SELECT
    m.*,
    c.customer_name, c.province, c.Region, c.customer_segment,
    o.order_date, o.order_priority, 
    p.product_category, p.product_sub_category,
    s.ship_mode, s.ship_date
FROM market_fact m
JOIN cust_dimen c using (cust_id)
JOIN orders_dimen o using (ord_id)
JOIN prod_dimen p using (prod_id)
JOIN shipping_dimen s using (ship_id);

select * from combined_table;

#2.	Find the top 3 customers who have the maximum number of orders
SELECT customer_name, order_quantity
FROM combined_table
GROUP BY customer_name, order_quantity
ORDER BY order_quantity DESC
LIMIT 3;

#3.Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
ALTER TABLE combined_table
ADD COLUMN DaysTakenForDelivery INT;

UPDATE combined_table
SET ship_date = STR_TO_DATE(ship_date, '%d-%m-%Y');
UPDATE combined_table
SET order_date = STR_TO_DATE(order_date, '%d-%m-%Y');

UPDATE combined_table
SET DaysTakenForDelivery = datediff(ship_date, order_date);


#4.	Find the customer whose order took the maximum time to get delivered
SELECT customer_name, MAX(DaysTakenForDelivery) AS max_delivery_time
FROM combined_table
GROUP BY customer_name
ORDER BY max_delivery_time DESC
LIMIT 1;

#5.	Retrieve total sales made by each product from the data (use Windows function)
SELECT
    prod_id,
    product_category,
    product_sub_category,
    SUM(sales) OVER (PARTITION BY prod_id ORDER BY order_date) AS total_sales
FROM combined_table
ORDER BY prod_id, order_date;

#6.	Retrieve total profit made from each product from the data (use windows function)

select prod_id, product_category, product_sub_category,
sum(profit) over(partition by prod_id order by order_date) as total_profit
from combined_table
order by prod_id, order_date;

#7.	Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
-- Count the total number of unique customers in January 2011
SELECT COUNT(DISTINCT cust_id) AS unique_customers_in_january
FROM combined_table
WHERE YEAR(order_date) = 2011 AND MONTH(order_date) = 1;

-- Count the number of customers who came back every month in 2011
SELECT COUNT(DISTINCT cust_id) AS customers_returned_every_month
FROM combined_table
WHERE YEAR(order_date) = 2011
GROUP BY cust_id
HAVING COUNT(DISTINCT MONTH(order_date)) = 12;


-- 8.	Retrieve month-by-month customer retention rate since the start of the business.(using views)

-- 1. Creating view for all customers and their visits
create view customer_visits as
select cust_id,str_to_date(order_date,"%d-%m-%Y") cust_visit 
from combined_table;

select * from customer_visits;

-- 2.creating view to check previous visit of every customer 
create view customer_visits_timelamp as
select cust_id ,year(cust_visit),cust_visit, lag(cust_visit) over(partition by cust_id 
order by cust_visit,year(cust_visit)) previous_visit
from customer_visits order by cust_id,cust_visit;

select * from customer_visits_timelamp;	

-- 3. creating view to check those customer who visited back next month
create view customer_time_gaps as
select cust_id, cust_visit, previous_visit
from customer_visits_timelamp where  datediff(cust_visit,previous_visit)<61 and month(cust_visit)-month(previous_visit) in (1,-11) and
month(cust_visit)!=month(previous_visit) order by cust_visit;

select * from customer_time_gaps;

-- 4. creating view to check number of customer visited every month and checking the customer retained.
create view monthly_cust_visit as
select *,lag(Monthly_customer_visit)over() previous_mnth_cust_visit
from (select year(cust_visit) year,month(cust_visit) month,count(cust_visit) Monthly_customer_visit
 from customer_visits_timelamp
 group by year(cust_visit),month(cust_visit) order by year(cust_visit)) t;
 
select * from monthly_cust_visit;
  
create view customer_retention as 
select year(cust_visit) year,month(cust_visit) Month, count(cust_visit) customer_retained
from customer_time_gaps group by year(cust_visit),month(cust_visit)
order by year,month;
 
select * from customer_retention;
 
-- 5 final answer checking thecustomer retention rate by joining the monthly_cust_visit and customer_retention
 select year,month,previous_mnth_cust_visit,customer_retained,(customer_retained/previous_mnth_cust_visit)*100 cust_retn_rate
 from customer_retention natural join monthly_cust_visit order by year;


select * from customer_visits;
select * from customer_visits_timelamp;
select * from customer_time_gaps;
select * from customer_retention;
select * from monthly_cust_visit;





