-- 1. Создание представления с общими продажами по месяцам

CREATE VIEW monthly_sales AS
SELECT 
    EXTRACT(YEAR FROM Order_Date) AS year,
    EXTRACT(MONTH FROM Order_Date) AS month,
    ROUND(SUM(Sales), 2) AS total_sales
FROM orders
GROUP BY year, month
ORDER BY year, month;

-- 2. Расчет количества заказов по категориям товаров

SELECT 
    Category,
    COUNT(DISTINCT Order_ID) AS order_count
FROM orders
GROUP BY Category
ORDER BY order_count DESC;

-- 3. Определение среднего чека по сегментам клиентов

SELECT 
    Segment,
    ROUND(AVG(order_total), 2) AS avg_order_value
FROM (
    SELECT 
        Segment,
        Order_ID,
        SUM(Sales) AS order_total
    FROM orders
    GROUP BY Segment, Order_ID
) AS order_totals
GROUP BY Segment
ORDER BY avg_order_value DESC;