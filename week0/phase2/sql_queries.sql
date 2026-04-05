-- Q1
SELECT customer_id, SUM(order_amount)
FROM orders
GROUP BY customer_id;

-- Q2
SELECT customer_id, SUM(order_amount) AS total
FROM orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 3;

-- Q3
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- Q4
SELECT c.city, SUM(o.order_amount)
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.city;

-- Q5
SELECT customer_id, AVG(order_amount)
FROM orders
GROUP BY customer_id;

-- Q6
SELECT customer_id, COUNT(*) as cnt
FROM orders
GROUP BY customer_id
HAVING cnt > 1;

-- Q7
SELECT customer_id, SUM(order_amount) AS total
FROM orders
GROUP BY customer_id
ORDER BY total DESC;
