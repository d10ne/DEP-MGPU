DROP VIEW IF EXISTS dm_v02_category_sales;
CREATE VIEW dm_v02_category_sales AS
SELECT purchase_month                AS month,              -- первый день месяца, ГГГГ-ММ-01
       category,                                            -- категория (англ.) или 'unknown'
       COUNT(*)                      AS items,              -- проданных позиций (штук)
       COUNT(DISTINCT order_id)      AS orders,             -- заказов с этой категорией в месяце
       ROUND(SUM(price), 2)          AS revenue,            -- выручка товаров, BRL (без доставки)
       ROUND(SUM(freight_value), 2)  AS freight             -- стоимость доставки, BRL
FROM core_item
WHERE order_status = 'delivered'                            -- только доставленные заказы
  AND purchase_month BETWEEN '2017-01-01' AND '2018-08-01'  -- полные месяцы (2016 и сен–окт 2018 неполные)
GROUP BY purchase_month, category;
