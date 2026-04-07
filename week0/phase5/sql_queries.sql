-- Task 1: Top 3 customers per city
SELECT customer_city, customer_id, total_spend, rank
FROM (
    SELECT c.customer_city, c.customer_id,
           SUM(p.payment_value) AS total_spend,
           ROW_NUMBER() OVER (PARTITION BY c.customer_city ORDER BY SUM(p.payment_value) DESC) AS rank
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN payments p ON o.order_id = p.order_id
    GROUP BY c.customer_city, c.customer_id
) t
WHERE rank <= 3;

-- Task 2: Running total
SELECT date, daily_sales,
       SUM(daily_sales) OVER (ORDER BY date) AS running_total
FROM (
    SELECT DATE(order_purchase_timestamp) AS date,
           SUM(payment_value) AS daily_sales
    FROM orders o
    JOIN payments p ON o.order_id = p.order_id
    GROUP BY date
) t;

-- Task 3: Top products per category
SELECT product_category_name, product_id, total_sales,
       DENSE_RANK() OVER (PARTITION BY product_category_name ORDER BY total_sales DESC) AS rank
FROM (
    SELECT pr.product_category_name, oi.product_id,
           SUM(oi.price) AS total_sales
    FROM order_items oi
    JOIN products pr ON oi.product_id = pr.product_id
    GROUP BY pr.product_category_name, oi.product_id
) t;
