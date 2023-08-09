{{ config(schema='transaction') }}

WITH
    ship  AS (SELECT * FROM {{ref('gz_stg_ship')}})
    , sales AS (SELECT * FROM {{ref('gz_stg_sales')}})
    -- Aggregation --
    ,orders_from_sales AS (
        SELECT
            orders_id
            ,date_date
            ,ROUND(SUM(turnover),2) AS turnover
            ,ROUND(SUM(purchase_cost),2) AS purchase_cost
            ,ROUND(SUM(pdt_margin),2) AS product_margin
        FROM sales
        GROUP BY
            orders_id
            , date_date
    )
    -- Join --
    ,orders_join AS (
        SELECT
            orders_id
            ,date_date
            -- revenue metrics --
            ,turnover
            ,purchase_cost
            ,orders_from_sales.product_margin
            -- log & ship metrics --
            ,shipping_fee
            ,ship_cost
            ,log_cost
        FROM orders_from_sales
        LEFT JOIN ship USING (orders_id))

    -- enrichment --
    ,operational_margin AS (
        SELECT
        *
        ,ROUND(product_margin + shipping_fee - ship_cost - log_cost,2) AS operational_margin 
    FROM orders_from_sales
    LEFT JOIN ship USING (orders_id)
    )

SELECT * FROM operational_margin