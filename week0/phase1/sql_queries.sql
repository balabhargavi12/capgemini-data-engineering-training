-- 1. Show all customers
SELECT * FROM customers;

-- 2. Customers from Chennai
SELECT * FROM customers WHERE city = 'Chennai';

-- 3. Customers with age > 25
SELECT * FROM customers WHERE age > 25;

-- 4. Show name and city
SELECT customer_name, city FROM customers;

-- 5. Count customers city-wise
SELECT city, COUNT(*) FROM customers GROUP BY city;
