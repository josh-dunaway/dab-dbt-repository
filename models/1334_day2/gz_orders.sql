WITH
    ship  AS (SELECT * FROM {{ref('gz_stg_ship')}})
    , sales AS (SELECT * FROM {{ref('gz_stg_sales')}})
    -- Aggregation --
    ,orders_from_sales AS (
        SELECT
            orders_id
            ,date_date
            ,SUM(turnover) AS turnover
            ,SUM(purchase_cost) AS purchase_cost
            ,SUM(pdt_margin) AS product_margin
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
        ,product_margin + shipping_fee - ship_cost - log_cost AS operational_margin 
    FROM orders_from_sales
    LEFT JOIN ship USING (orders_id)
    )

SELECT * FROM operational_margin