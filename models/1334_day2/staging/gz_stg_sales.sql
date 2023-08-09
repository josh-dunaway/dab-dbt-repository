--{{ config(schema='staging') }}

SELECT
  sales.date_date
  ,sales.orders_id
  ,sales.pdt_id AS products_id
  ,sales.revenue AS turnover
  ,sales.quantity AS qty
  ,sales.quantity * product.purchase_price AS purchase_cost
  ,sales.revenue - (sales.quantity * product.purchase_price) AS pdt_margin
FROM `gz_raw_data.raw_gz_sales` sales
LEFT JOIN {{ref('gz_stg_product')}} product ON (product.products_id = sales.pdt_id)
WHERE revenue > 0