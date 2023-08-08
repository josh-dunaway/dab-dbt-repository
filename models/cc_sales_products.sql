{{
  config(
    materialized='table'
  )
}}

SELECT
  ### Key ###  
  product_id
  ###########
  ,SUM(quantity) AS qty_91
  ,ROUND(SUM(quantity)/91,2) AS avg_daily_qty_91
FROM `raw_data_circle.raw_cc_sales` 
WHERE 
	date_date >= DATE_SUB('2021-10-01',INTERVAL 91 DAY)
GROUP BY product_id