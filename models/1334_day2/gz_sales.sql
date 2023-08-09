{{ config(schema='transaction') }}

WITH 

    sales AS (SELECT * FROM {{ref('gz_stg_sales')}})

    ,product AS (SELECT * FROM {{ref('gz_stg_product')}})

SELECT
    s.date_date
    ### Key ###
    ,s.orders_id
    ,s.products_id AS products_id
    ###########
    -- qty --
    ,s.qty AS qty
    -- revenue --
    ,s.turnover AS turnover
    -- cost --
    ,CAST(p.purchase_price AS FLOAT64) AS purchase_price
    ,ROUND(s.qty*CAST(p.purchase_price AS FLOAT64),2) AS purchase_cost
    -- margin --
    ,{{margin('s.turnover','s.qty*CAST(p.purchase_price AS FLOAT64)')}} AS margin
    ,{{margin_percent('s.turnover','s.qty*CAST(p.purchase_price AS FLOAT64)')}} AS margin_percent
FROM sales s
INNER JOIN product p ON s.products_id = p.products_id