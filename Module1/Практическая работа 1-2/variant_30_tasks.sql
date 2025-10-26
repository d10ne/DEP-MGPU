-- Задание 1: Определить выручку по подкатегориям
-- Цель: Рассчитать общую выручку для каждой товарной подкатегории на основе данных из таблицы заказов.

SELECT 
    subcategory,
    SUM(profit) AS Total_Revenue
FROM public.orders
GROUP BY subcategory
ORDER BY Total_Revenue DESC;

-- Задание 2: Создать таблицу по возвратам регионов
-- Цель: Сформировать новую таблицу dw.Region_Returns, содержащую количество возвратов по каждому региону на основе данных из таблицы возвратов.

DROP TABLE IF EXISTS dw.Region_Returns;

CREATE TABLE dw.Region_Returns AS
SELECT 
    region,
    COUNT(*) AS Return_Count
FROM public.returns
GROUP BY region
ORDER BY Return_Count DESC;

SELECT * FROM dw.Region_Returns;

-- Задание 3: Найти средний чек по менеджерам
-- Цель: Рассчитать средний чек для каждого менеджера, используя связь между заказами и менеджерами через регион.

SELECT 
    p.person AS Manager_Name,
    p.region AS Region,
    COUNT(DISTINCT o.order_id) AS Total_Orders,
    SUM(o.sales) AS Total_Sales,
    AVG(o.sales) AS Average_Check_Per_Order
FROM public.orders o
JOIN public.people p ON o.region = p.region
GROUP BY p.person, p.region
ORDER BY Average_Check_Per_Order DESC;