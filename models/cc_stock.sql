SELECT
  ### Key ###
  CONCAT(model,"_",color,"_",IFNULL(size,"no-size")) AS product_id 
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
  ,CONCAT(model_name," ",color_name,IF(size IS NULL,"",CONCAT(" - Taille ",size))) AS product_name
  -- product info --
  ,t.new AS pdt_new
  -- stock metrics --
  ,forecast_stock
  ,stock
	,IF(stock>0,1,0) AS in_stock
	-- value
  ,price
	,IF(stock<0,NULL,ROUND(stock*price,2)) AS stock_value
FROM `raw_data_circle.raw_cc_stock` t
WHERE TRUE
ORDER BY product_id