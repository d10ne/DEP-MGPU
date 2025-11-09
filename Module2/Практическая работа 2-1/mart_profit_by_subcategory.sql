-- mart_profit_by_subcategory.sql
-- Анализ прибыльности по категориям

-- Анализ прибыльности по категориям 
WITH profit_analysis AS (
    SELECT 
        category,
        subcategory,
        SUM(profit) AS total_profit,
        SUM(sales) AS total_sales,
        COUNT(DISTINCT order_id) AS order_count
    FROM orders
    GROUP BY category, subcategory
),
ranked_categories AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_profit DESC) AS profit_rank,
        RANK() OVER (ORDER BY total_profit ASC) AS loss_rank
    FROM profit_analysis
)
-- Самые прибыльные
SELECT 'Самые прибыльные' as analysis_type, *
FROM ranked_categories 
WHERE profit_rank <= 3
UNION ALL
-- Самые убыточные
SELECT 'Самые убыточные' as analysis_type, *
FROM ranked_categories 
WHERE loss_rank <= 3
ORDER BY analysis_type DESC, total_profit DESC