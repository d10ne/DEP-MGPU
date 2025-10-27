-- Анализ Парето: какой процент общей выручки приносят 20% самых прибыльных клиентов?

WITH customer_revenue AS (
    -- Выручка по каждому клиенту
    SELECT 
        customer_id,
        customer_name,
        SUM(sales) AS customer_total_revenue
    FROM public.orders
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, customer_name
),
total_stats AS (
    -- Oбщая выручка и количество клиентов
    SELECT 
        COUNT(*) AS total_customers,
        SUM(customer_total_revenue) AS total_revenue
    FROM customer_revenue
),
top_20_percent AS (
    -- Oтбoр топ-20% клиентов по выручке
    SELECT 
        customer_id,
        customer_name,
        customer_total_revenue,
        ROW_NUMBER() OVER (ORDER BY customer_total_revenue DESC) AS rn
    FROM customer_revenue
),
top_customers AS (
    SELECT 
        t.customer_id,
        t.customer_name,
        t.customer_total_revenue
    FROM top_20_percent t
    CROSS JOIN total_stats s
    WHERE t.rn <= CEIL(s.total_customers * 0.20)
)
-- Итог
SELECT 
    (SELECT COUNT(*) FROM top_customers) AS top_20pct_customer_count,
    (SELECT total_customers FROM total_stats) AS total_customer_count,
    ROUND(
        (SELECT COUNT(*) FROM top_customers)::NUMERIC / (SELECT total_customers FROM total_stats) * 100, 
        2
    ) AS actual_top_pct,
    ROUND(
        (SELECT SUM(customer_total_revenue) FROM top_customers), 
        2
    ) AS revenue_from_top_20pct,
    ROUND(
        (SELECT total_revenue FROM total_stats), 
        2
    ) AS total_revenue,
    ROUND(
        (SELECT SUM(customer_total_revenue) FROM top_customers) / (SELECT total_revenue FROM total_stats) * 100, 
        2
    ) AS pct_of_revenue_from_top_20pct