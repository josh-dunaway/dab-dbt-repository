WITH cc_sales_products AS (
    SELECT * FROM {{ ref('cc_sales_products') }}
  ),
stg_cc_stock AS (
  SELECT
    ### Key ###
    CONCAT(model,"_",color,"_",IFNULL(size,"no-size")) AS product_id 
    ###########
    ,model
    ,color
    ,size
    -- name
    ,model_name
    ,color_name
    ,CONCAT(model_name," ",color_name,IF(size IS NULL,"",CONCAT(" - Taille ",size))) AS product_name
    -- product info --
    ,t.new AS pdt_new
    -- stock metrics --
    ,forecast_stock
    ,stock
    -- value
    ,price
  FROM `raw_data_circle.raw_cc_stock` t
)
SELECT
  ### Key ###
  product_id 
  ###########
  ,model
  ,color
  ,size
  -- category
  ,CASE
    WHEN REGEXP_CONTAINS(LOWER(model_name),'t-shirt') THEN 'T-shirt'
    WHEN REGEXP_CONTAINS(LOWER(model_name),'short') THEN 'Short'
    WHEN REGEXP_CONTAINS(LOWER(model_name),'legging') THEN 'Legging'
    WHEN REGEXP_CONTAINS(LOWER(REPLACE(model_name,"è","e")),'brassiere|crop-top') THEN 'Crop-top'
    WHEN REGEXP_CONTAINS(LOWER(model_name),'débardeur|haut') THEN 'Top'
    WHEN REGEXP_CONTAINS(LOWER(model_name),'tour de cou|tapis|gourde') THEN 'Accessories'
    ELSE NULL
  END AS model_type
  -- name
  ,model_name
  ,color_name
  ,product_name
  -- product info --
  ,pdt_new
  -- stock metrics --
  ,forecast_stock
  ,stock
	,IF(stock>0,1,0) AS in_stock
	-- value
  ,price
	,IF(stock<0,NULL,ROUND(stock*price,2)) AS stock_value
  -- nb days --
  ,d.avg_daily_qty_91
  ,SAFE_DIVIDE(t.stock,d.avg_daily_qty_91) AS nb_day_stock
FROM stg_cc_stock t
LEFT JOIN cc_sales_products d USING (product_id)
WHERE TRUE
ORDER BY product_id