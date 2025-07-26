# Querys SQL
-- Análise Comportamental de Vendas por Horário e Ticket Médio

WITH Base AS (
  SELECT
    s.sales_creator_id AS CreatorID,
    CAST(s.sales_date_payment AS Date) AS DatePayment,
    HOUR(s.sales_date_payment) AS Hour,
    LOWER(LTRIM(SPLIT(p.product_category_name, ';')[0])) AS Category,
    l.SubCategory,
    s.sales_amount AS Invoicing,
    p.product_type AS ProductType,
    s.sales_lead_id AS LeadID

  FROM silver_layer.sales s
  INNER JOIN bronze_layer.products p ON s.product_main_id = p.product_id
  LEFT JOIN bronze_layer.leads l ON s.sales_creator_id = l.LeadCreatorID
  WHERE s.payment_status IN (3, 6, 7)
    AND s.sales_net_amount <> 0
    AND IFNULL(s.sales_is_test, 0) = 0
    AND MONTH(s.sales_date_payment) IN (1, 2, 3, 4, 5, 6, 7)
    AND YEAR(s.sales_date_payment) IN (2023, 2024, 2025)
    AND LOWER(p.product_category_name) = 'education and careers'
    AND l.SubCategory IS NOT NULL
),

TicketMedian AS (
  SELECT
    COUNT(DISTINCT CreatorID) AS TotalCreators,
    DatePayment,
    Hour,
    Category,
    SubCategory,
    ProductType,
    percentile_approx(Invoicing, 0.5) AS MedianTicket
  FROM Base
  GROUP BY CreatorID, DatePayment, Hour, Category, SubCategory, ProductType
),

Aggregated AS (
  SELECT
    COUNT(DISTINCT CreatorID) AS TotalCreators,
    DatePayment,
    Hour,
    Category,
    SubCategory,
    ProductType,
    SUM(Invoicing) AS TotalInvoicing,
    COUNT(*) AS TotalSales,
    COUNT(DISTINCT LeadID) AS UniqueLeads
  FROM Base
  GROUP BY CreatorID, DatePayment, Hour, Category, SubCategory, ProductType
)

SELECT
  SUM(a.TotalCreators) AS ProductsCount,
  a.DatePayment,
  CASE 
    WHEN a.Hour = 0 THEN '00:00'
    WHEN a.Hour = 1 THEN '01:00'
    WHEN a.Hour = 2 THEN '02:00'
    WHEN a.Hour = 3 THEN '03:00'
    WHEN a.Hour = 4 THEN '04:00'
    WHEN a.Hour = 5 THEN '05:00'
    WHEN a.Hour = 6 THEN '06:00'
    WHEN a.Hour = 7 THEN '07:00'
    WHEN a.Hour = 8 THEN '08:00'
    WHEN a.Hour = 9 THEN '09:00'
    WHEN a.Hour = 10 THEN '10:00'
    WHEN a.Hour = 11 THEN '11:00'
    WHEN a.Hour = 12 THEN '12:00'
    WHEN a.Hour = 13 THEN '13:00'
    WHEN a.Hour = 14 THEN '14:00'
    WHEN a.Hour = 15 THEN '15:00'
    WHEN a.Hour = 16 THEN '16:00'
    WHEN a.Hour = 17 THEN '17:00'
    WHEN a.Hour = 18 THEN '18:00'
    WHEN a.Hour = 19 THEN '19:00'
    WHEN a.Hour = 20 THEN '20:00'
    WHEN a.Hour = 21 THEN '21:00'
    WHEN a.Hour = 22 THEN '22:00'
    WHEN a.Hour = 23 THEN '23:00'
  END AS HourSlot,
  CASE 
    WHEN t.MedianTicket < 500 THEN '1 - From $100 to $500'
    WHEN t.MedianTicket BETWEEN 500 AND 1000 THEN '2 - From $500 to $1,000'
    WHEN t.MedianTicket BETWEEN 1000 AND 2000 THEN '3 - From $1,000 to $2,000'
    WHEN t.MedianTicket BETWEEN 2000 AND 3000 THEN '4 - From $2,000 to $3,000'
    WHEN t.MedianTicket BETWEEN 3000 AND 4000 THEN '5 - From $3,000 to $4,000'
    ELSE '6 - Above $4,000'
  END AS TicketRange,
  a.Category,
  a.SubCategory,
  SUM(a.TotalInvoicing) AS Invoicing,
  a.ProductType,
  SUM(a.TotalSales) AS SalesCount,
  SUM(a.UniqueLeads) AS UniqueLeadsCount

FROM Aggregated a
JOIN TicketMedian t ON a.DatePayment = t.DatePayment 
  AND a.Category = t.Category 
  AND a.SubCategory = t.SubCategory
  AND a.ProductType = t.ProductType
GROUP BY ALL
ORDER BY a.DatePayment ASC, HourSlot ASC;
