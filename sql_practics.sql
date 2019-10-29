drop table if exists teamanalytics.scoyne_user_dim;
create table teamanalytics.scoyne_user_dim(user_id int, account_id int);
insert into teamanalytics.scoyne_user_dim values
(1, 10),
(2, 20),
(3, 30),
(4, 40),
(5, 50);


drop table if exists teamanalytics.scoyne_account_dim;
create table teamanalytics.scoyne_account_dim(account_id int, is_paying_customer boolean);
insert into teamanalytics.scoyne_account_dim values
(10, true),
(20, false),
(30, false),
(40, true),
(50, true);

drop table if exists teamanalytics.scoyne_dload_facts;
create table teamanalytics.scoyne_dload_facts(date string, user_id int, downloads int);
insert into teamanalytics.scoyne_dload_facts values
("2019-01-01", 1, 30),
("2019-01-02", 1, 20),
("2019-01-03", 1, 10),
("2019-01-04", 1, 40),
("2019-01-05", 1, 50),
("2019-01-01", 2, 130),
("2019-01-02", 2, 120),
("2019-01-03", 2, 110),
("2019-01-04", 2, 140),
("2019-01-05", 2, 150);

--1) Select employee from departments where max salary of the department is 40k 

select 
  employee,
from employee e
inner join departments d
on e.id = d.employee_id
group by employee, d.department
having max(d.salary) <= 40k

--2) Select employee assigned to projects 
select 
  first_name,
  last_name
from employees e
inner join employees_projects ep
  on e.id = ep.employee_id
  
  --3) Select employee which have the max salary in a given department 
select 
  first_name,
  last_name,
  salary,
  department_id
from employees e
where department_id = 1
order by salary desc
limit 1 

--4) Select employee with second highest salary 
select 
  first_name,
  last_name,
  salary,
  department_id
from employees e
where department_id = 1
order by salary desc
limit 1 offset 1

--OFFSET 1 is the key











drop table if exists teamanalytics.scouyne_apples;
create table teamanalytics.scouyne_apples(date string, type string, sold int);
insert into teamanalytics.scouyne_apples values
("2019-01-01", "Apples", 100),
("2019-01-01", "Oranges", 44),
("2019-01-02", "Apples", 10),
("2019-01-02", "Oranges", 93),
("2019-01-03", "Apples", 55),
("2019-01-03", "Oranges", 55)


--5) Table has two data entries every day for # of apples and oranges sold. write a query to get the difference between the apples and oranges sold on a given day

select 
  date,
  sum(case when type = "Apples" then sold else 0 end) as applesSold,
  sum(case when type = "Oranges" then sold else 0 end) as orangesSold,
  abs(sum(case when type = "Apples" then sold else 0 end) - sum(case when type = "Oranges" then sold else 0 end)) as differenceInSales
from teamanalytics.scouyne_apples
group by date
order by date 


-- get users that dont have any downloads
select
u.user_id
from teamanalytics.scoyne_user_dim u
LEFT JOIN teamanalytics.scoyne_dload_facts d
ON u.user_id = d.user_id
where d.user_id is null

--find the average number of downloads for free vs paying customers broken out by day.
SELECT
  SUM(CASE WHEN is_paying_customer = false THEN d.downloads ELSE NULL END)  as free_customer_downloads,
  SUM(CASE WHEN is_paying_customer = true THEN d.downloads ELSE NULL END)  as paying_customer_downloads,
  round(SUM(CASE WHEN is_paying_customer = false THEN d.downloads ELSE NULL END) / SUM(d.downloads),2) as pct_downloads_from_free_customers,
  round(SUM(CASE WHEN is_paying_customer = true THEN d.downloads ELSE NULL END) / SUM(d.downloads),2) as pct_downloads_from_paying_customers,
  d.date
FROM teamanalytics.scoyne_dload_facts d
INNER JOIN teamanalytics.scoyne_user_dim u
  ON d.user_id = u.user_id
INNER JOIN teamanalytics.scoyne_account_dim a
  ON a.account_id = u.account_id
GROUP BY d.date
ORDER BY d.date DESC



drop table if exists teamanalytics.scoyne_soda_orders;
create table teamanalytics.scoyne_soda_orders(order_id int, customer_id int, product_id string);
insert into teamanalytics.scoyne_soda_orders values
(1, 1, 1),
(2, 1, 1),
(3, 1, 1),
(4, 1, 2),
(5, 1, 2),
(6, 2, 1),
(7, 2, 1),
(8, 2, 1),
(9, 2, 1),
(10, 2, 3),
(11, 2, 3),
(12, 3, 1),
(13, 4, 2),
(14, 5, 3),
(15, 5, 3),
(16, 6, 1)


drop table if exists teamanalytics.scoyne_products;
create table teamanalytics.scoyne_products(product_id int, brand_name string, product_name string, product_price decimal(10,2), class_id string);
insert into teamanalytics.scoyne_products values
(1, "Pepsi", "Cola", .99,1),
(2, "Pepsi", "Diet Cola", .99,2),
(3, "Pepsi", "MtDew", .99,2),

(4, "CocaCola", "Coke", 4.99,1),
(5, "CocaCola", "Diet Coke", 4.99,2),
(6, "CocaCola", "Sprite", 4.99,3),

(7, "Natural", "Cola", 4.99,1),
(8, "Natural", "Diet Cola", 4.99,2),
(9, "Natural", "Mr Pepper", 4.99,3),
(10, "Natural", "Morning Dew", 4.99,3),
(11, "Natural", "Sparkle Water", 4.99,3),
(12, "Natural", "Water", 4.99,2),

(7, "SeanCola", "Cola", 6.99,1),
(8, "SeanCola", "Diet Cola", 6.99,2),
(9, "SeanCola", "Mr Pepper", 6.99,3),
(10, "SeanCola", "Morning Dew", 6.99,2),
(11, "SeanCola", "Sparkle Water", 6.99,3),
(12, "SeanCola", "Water", 6.99,5),

(7, "BeckyCola", "Cola", 5.99,1),
(8, "BeckyCola", "Diet Cola", 5.99,2),
(9, "BeckyCola", "Mr Pepper", 5.99,3),
(10, "BeckyCola", "Morning Dew", 5.99,3),
(11, "BeckyCola", "Sparkle Water", 5.99,3),
(12, "BeckyCola", "Water", 5.99,3),

(7, "BrianCola", "Cola", 3.99,1),
(8, "BrianCola", "Diet Cola", 3.99,2),
(9, "BrianCola", "Mr Pepper", 3.99,3),
(10, "BrianCola", "Morning Dew", 3.99,3),
(11, "BrianCola", "Sparkle Water", 3.99,4),
(12, "BrianCola", "Water", 3.99,3);



--most common product name
select
product_name, count(*)
from teamanalytics.scoyne_products
group by 1
order by count(*) desc
limit 1


--3. Get most ordered products for each category
with orders_by_product as(
SELECT distinct
  class_id,
  product_name, 
  count(*) as OrderCount
FROM teamanalytics.scoyne_products
group by 1,2)


SELECT * FROM(
SELECT 
     p.class_id,
     p.product_name,
     p.OrderCount,
     rank() over (partition by class_id order by OrderCount desc) as rank
FROM orders_by_product p
)
WHERE rank =1
ORDER BY 1 asc


-- 1. given table products(product_id, brand_name,product_name, product_price), find brand_names with avg price > 3 and having more then 5 products
SELECT
  brand_name
FROM teamanalytics.scoyne_products
GROUP BY 1
HAVING avg(product_price) > 3
AND count(DISTINCT product_id) > 5


--Top 3 brand names with their average prices more than $3 and having atleast 2 products.  
SELECT
  brand_name,
  avg(product_price) AS average_price
FROM teamanalytics.scoyne_products
GROUP BY 1
HAVING avg(product_price) > 3
  AND count(DISTINCT product_id) >= 2
ORDER BY 2 DESC
LIMIT 3


drop table if exists teamanalytics.scoyne_sales;
create table teamanalytics.scoyne_sales(sale_id int, promotion_id string, sale_amount decimal(10,2), product string, date string);
insert into teamanalytics.scoyne_sales values
(1, null, 99.99, "tv", "2019-01-01"),
(2, "promo1", 88.99, "fridge", "2019-01-02"),
(3, "promo1", 88.99, "fridge", "2019-01-03"),
(4, null, 99.99, "microwave", "2019-02-01"),
(5, "promo1", 99.99, "fridge", "2019-02-01"),
(6, "promo1", 88.99, "tv", "2019-02-01"),
(7, "promo1", 88.99, "tv", "2019-02-02"),
(8, "promo1", 88.99, "microwave", "2019-02-03"),
(9, "promo1", 99.99, "microwave", "2019-02-04"),
(10, null, 99.99, "tv", "2019-02-05");

-- top 3 products by sale 
SELECT
  product,
  SUM(sale_amount) AS product_sales  
FROM teamanalytics.scoyne_sales
GROUP BY product
ORDER BY 2 DESC
LIMIT 3


--  % of how sales with promotions ( promotion_id is null) are doing in comparison to all sales.
SELECT
  SUM(CASE WHEN promotion_id IS NULL THEN 0 ELSE sale_amount END) AS sales_with_promotions,
  SUM(CASE WHEN promotion_id IS NULL THEN sale_amount ELSE 0 END) AS sales_without_promotions,
  SUM(sale_amount) AS sales_total,
  ROUND(SUM(CASE WHEN promotion_id IS NULL THEN 0 ELSE sale_amount END) / SUM(sale_amount), 4)*100 AS sales_pct_promotions  
FROM teamanalytics.scoyne_sales


-- 2. product that has the highest average price (top 2) class_id
SELECT
  product,
  AVG(sale_amount) as avg_price
FROM teamanalytics.scoyne_sales
GROUP BY product
ORDER BY 2 DESC
LIMIT 2



drop table if exists teamanalytics.scoyne_salesperson;
create table teamanalytics.scoyne_salesperson(id int, name string, age int, salary int);
insert into teamanalytics.scoyne_salesperson values
(1, "Abe", 61, 140000),
(2, "Bob", 34, 44000),
(5, "Chris", 34, 40000),
(7, "Dan", 41, 52000),
(8, "Ken", 57, 115000),
(11, "Joe", 38, 38000);


drop table if exists teamanalytics.scoyne_customer;
create table teamanalytics.scoyne_customer(id int, name string, city string, industry_type string);
insert into teamanalytics.scoyne_customer values
(4, "Samsonic", "pleasant", "J"),
(6, "Panasung", "oaktown", "J"),
(7, "Samony", "jackson", "B"),
(9, "Orange", "Jackson", "B");

drop table if exists teamanalytics.scoyne_orders;
create table teamanalytics.scoyne_orders(number int, order_Date date, cust_id int, salesperson_id int, amount int);
insert into teamanalytics.scoyne_orders values
(10, "1996-08-02", 4,  2, 540),
(20, "1999-01-30", 4, 8, 1800),
(30, "1995-07-14", 9, 1, 460),
(40, "1998-01-29", 7, 2, 2400),
(50, "1998-02-03", 6,  7, 600),
(60, "1998-03-02", 6,  7, 720),
(70, "1998-05-06", 9,  7, 150);


-- 7 what is the total percentage of sale of a Samsonic sales compared to all sales.
SELECT
  ROUND(SUM(CASE WHEN c.name = 'Samsonic' THEN o.amount ELSE 0 END) / SUM(amount),4) *100 as pct_sales
FROM teamanalytics.scoyne_orders o
INNER JOIN teamanalytics.scoyne_customer c
ON o.cust_id = c.id

-- 6 find the sales people with average sale amount greater 1000 with at least 2 companies
SELECT
  p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON p.id = o.salesperson_id
INNER JOIN teamanalytics.scoyne_customer c
  ON c.id = o.cust_id
GROUP BY p.name
HAVING avg(o.amount) > 1000
AND COUNT(DISTINCT c.id ) >=2


-- 3.1 names of salespeople who had only Samsonic and Samony as customers
SELECT
 p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON p.id = o.salesperson_id
INNER JOIN teamanalytics.scoyne_customer c
  ON c.id = o.cust_id
WHERE c.name in ("Samsonic",  "Samony")
GROUP BY p.name
HAVING COUNT(DISTINCT c.id ) =2

-- 3.2 names of salespeople who had two customers
SELECT
 p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON p.id = o.salesperson_id
INNER JOIN teamanalytics.scoyne_customer c
  ON c.id = o.cust_id
GROUP BY p.name
HAVING COUNT(DISTINCT c.id ) =2

--names of all salespeople that have an order with samsonic
SELECT
 DISTINCT p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON p.id = o.salesperson_id
INNER JOIN teamanalytics.scoyne_customer c
  ON c.id = o.cust_id
WHERE c.name = "Samsonic"  

--names of all salespeople that do not have an order with samsonic
select 
  distinct s.name
from teamanalytics.scoyne_salesperson s
inner join teamanalytics.scoyne_orders o
  on o.salesperson_id = s.id
inner join teamanalytics.scoyne_customer c
  on c.id = o.cust_id
where not exists(
  select 
    distinct s2.name
  from teamanalytics.scoyne_salesperson s2
  inner join teamanalytics.scoyne_orders o2
    on o2.salesperson_id = s2.id
  inner join teamanalytics.scoyne_customer c2
    on c2.id = o2.cust_id
  where c2.name = "Samsonic"
  and s2.name = s.name
)

-- names of salespeople with two or more orders
SELECT
  p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON o.salesperson_id = p.id
GROUP BY 1
HAVING COUNT(DISTINCT o.number) >= 2

-- names and ages of all salespeople with a salary of 100k or greater
SELECT
  name
  age
FROM teamanalytics.scoyne_salesperson
WHERE salary >= 100000

--names of sales people that sold over 1400 units
SELECT
  p.name
FROM teamanalytics.scoyne_salesperson p
INNER JOIN teamanalytics.scoyne_orders o
  ON o.salesperson_id = p.id
GROUP BY 1
HAVING SUM(amount) > 1400


--when was earliest and latest order made to samony
SELECT
  MIN(o.order_Date) AS first_order,
  MAX(o.order_Date) AS last_order
FROM teamanalytics.scoyne_orders o
  INNER JOIN teamanalytics.scoyne_customer c
  ON c.id = o.cust_id


