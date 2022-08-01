CREATE OR REPLACE TABLE
  `dw-bq-data-d00.SANDBOX_ANALYTICS.sales_transaction_distinct_skus` AS
SELECT 
     
     distinct a.item_sku_num, 
     ROW_NUMBER() OVER (ORDER BY a.item_sku_num) AS transaction_guid,
     ROW_NUMBER() OVER (ORDER BY a.item_sku_num) AS customer_id,
     1 as buy_flag,
     a.pdm_prod_type_id, 
     a.item_sku_desc,
     pdm_prod_type_desc,  
     d.l1,d.l2,d.l3,d.l4,
     c.item_category_cd AS item_category_cd,
     ic.item_category_desc AS item_category_desc,
     c.department_cd AS department_cd,
     c.department_id AS department_id,
     c.department_name AS department_name,
     sd.sub_department_cd AS sub_department_cd,
     sd.sub_department_id AS sub_department_id,
     sd.sub_department_name AS sub_department_name,
     cc.class_cd AS class_cd,
     cc.class_name AS class_name
     FROM `dw-bq-data-p00.COMM_MCF_TB.ITEM_SKU` A
     join `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE`  B
     on A.PDM_PROD_TYPE_ID = B.PDM_PROD_TYPE_ID
     join `dw-bq-data-d00.SANDBOX_ANALYTICS.sku_l1l2l3_all` D
     on A.ITEM_SKU_NUM=d.ITEM_SKU_NUM
     INNER JOIN `dw-bq-data-p00.EDW_MCF_VW.CLASS_CODE` AS cc
     ON cc.class_id = A.class_id
     INNER JOIN `dw-bq-data-p00.EDW_MCF_VW.SUB_DEPARTMENT` AS sd
     ON cc.sub_department_id = sd.sub_department_id
     INNER JOIN `dw-bq-data-p00.EDW_MCF_VW.DEPARTMENT` AS c
     ON c.department_id = sd.department_id
     INNER JOIN `dw-bq-data-p00.EDW_MCF_VW.ITEM_CATEGORY` AS ic
     ON c.item_category_cd = ic.item_category_cd
     where A.PDM_PROD_TYPE_ID > 0
     and PDM_PROD_TYPE_DESC  != 'UNKNOWN';




select l1,count(distinct pdm_prod_type_id) from `dw-bq-data-d00.SANDBOX_ANALYTICS.sales_transaction_distinct_skus` group by 1;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.base_table` as
SELECT 
customer_id,
transaction_guid, 
pdm_prod_type_desc, 
pdm_prod_type_id, 
point_of_sale_item_sku_num as item_sku_num,
item_sku_desc,
l1,
l2,
l3, 
l4,
transaction_booked_date, 
sku_price,
sold_units,
1 as buy_flag
from 
(
select 
cu.customer_id,
f.transaction_guid,
s.pdm_prod_type_desc,
s.pdm_prod_type_id,
s.item_sku_desc,
s.l1,
s.l2,
s.l3,
s.l4,
f.point_of_sale_item_sku_num,
f.transaction_booked_date, 
f.gross_sales/f.sold_units as sku_price,
f.sold_units
FROM
    /*Transaction Line */
    (
    select 
    transaction_guid,
    point_of_sale_item_sku_num,
    transaction_booked_date, 
    gross_sales,
    concept_format_id,
    sold_units
     from `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_line_sum` 
    where transaction_booked_date between date_sub(current_date(), interval 6 MONTH) and current_date()
    ) f 

    /* Getting Customer_ID */
	left join  `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_TXN_XREF`  cu 
    on cu.trans_id=f.transaction_guid
	left join `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_CURR` ch 
    on cu.customer_id=ch.customer_id

    /* SKU Related Info */
	LEFT JOIN 
    (SELECT 
     distinct a.item_sku_num, 
     a.pdm_prod_type_id, 
     a.item_sku_desc,
     pdm_prod_type_desc,  
     d.l1,d.l2,d.l3,d.l4,
     FROM `dw-bq-data-p00.COMM_MCF_TB.ITEM_SKU` A
     join `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE`  B
     on A.PDM_PROD_TYPE_ID = B.PDM_PROD_TYPE_ID
     join `dw-bq-data-d00.SANDBOX_ANALYTICS.sku_l1l2l3_all` D
     on A.ITEM_SKU_NUM=d.ITEM_SKU_NUM
     where A.PDM_PROD_TYPE_ID > 0
     and PDM_PROD_TYPE_DESC  != 'UNKNOWN'
    ) S 
    ON f.point_of_sale_item_sku_num=S.item_sku_num
    /* Filters */
    where f.concept_format_id=1 and ch.customer_purge_ind='N' 
    AND cu.customer_id>0 
	AND f.transaction_booked_date between DATE_SUB(current_date(), INTERVAL 6 MONTH) and current_date()
    AND S.PDM_PROD_TYPE_ID is not null
     group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) A;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table` as 
SELECT 
customer_id, 
pdm_prod_type_id, 
count(transaction_guid) as trans_count
from
`dw-bq-data-d00.SANDBOX_ANALYTICS.base_table`
group by 1,2
order by 1,3 desc;


create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` as
select a.customer_id,
case when TRIM(UPPER(TERRITORY_CD)) in ('IA','IL','IN','KS','MI','MN','MO','ND','NE','OH','SD','WI') then "Midwest"
      when TRIM(UPPER(TERRITORY_CD)) in ('ME','NH','NJ','NY','PA','RI','VT') then "Northeast"      
      when TRIM(UPPER(TERRITORY_CD)) in ('AL','AR','DC','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV') then "South" 
      when TRIM(UPPER(TERRITORY_CD)) in ('AK','AZ','CA','CO','HI','ID','MT','NM','NV','OR','UT','WA','WY') then "West" 
      when TRIM(TERRITORY_CD) is null then "UNKNOWN"
      else "UNKNOWN"
      end as State_Division
from
(
select customer_id, count(*) from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table` group by 1  having count(*)>1

) a
LEFT JOIN 
    (
        SELECT CUSTOMER_ID,TERRITORY_CD 
        FROM 
        `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_ADDRESS` a
        LEFT JOIN
        `dw-bq-data-p00.EDW_MCF_VW.ADDRESS_MSTR` b
        on a.ADDRESS_ID=b.ADDRESS_ID
        LEFT JOIN
        `dw-bq-data-p00.EDW_MCF_VW.MAILING_ADDRESS` c
        on b.MAILING_ADDRESS_ID=c.MAILING_ADDRESS_ID
        where  PREFD_CUST_IND = 'Y'
    ) U
    on a.customer_id=U.CUSTOMER_ID ;



create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_Midwest` as
select distinct  customer_id, pdm_prod_type_id, trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
where customer_id in (select customer_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` where State_Division="Midwest")
and trans_count is not null
group by 1,2,3
union all
select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
) a;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_UNKNOWN` as
select distinct  customer_id, pdm_prod_type_id, trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
where customer_id in (select customer_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` where State_Division="UNKNOWN")
and trans_count is not null
group by 1,2,3
union all
select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
) a;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_West` as
select distinct  customer_id, pdm_prod_type_id, trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
where customer_id in (select customer_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` where State_Division="West")
and trans_count is not null
group by 1,2,3
union all
select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
) a;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_South` as
select distinct  customer_id, pdm_prod_type_id, trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
where customer_id in (select customer_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` where State_Division="South")
and trans_count is not null
group by 1,2,3
union all
select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
) a;


create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_Northeast` as
select distinct  customer_id, pdm_prod_type_id, trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
where customer_id in (select customer_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.multi_trans` where State_Division="Northeast")
and trans_count is not null
group by 1,2,3
union all
select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
) a;

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.prod_type_ref` as
select focus_pdm_prod_type_id,product_type_rank,recomm_pdm_prod_type_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendations`  where product_type_rank<=4 and sku_rank<=1
group by 1,2,3
union all 
select focus_pdm_prod_type_id,0 as product_type_rank,focus_pdm_prod_type_id as recomm_pdm_prod_type_id from `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendations`
group by 1,2,3
order by focus_pdm_prod_type_id,product_type_rank;

create or replace model `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_Northeast`
options (model_type = 'matrix_factorization',
user_col = 'customer_id',
item_col = 'pdm_prod_type_id',
rating_col = 'trans_count',
feedback_type = 'IMPLICIT'
) as
SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_Northeast`;

create or replace model `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_midwest`options (model_type = 'matrix_factorization',
user_col = 'customer_id',
item_col = 'pdm_prod_type_id',
rating_col = 'trans_count',
feedback_type = 'IMPLICIT'
) as
SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_Midwest`;

create or replace model `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_South`options (model_type = 'matrix_factorization',
user_col = 'customer_id',
item_col = 'pdm_prod_type_id',
rating_col = 'trans_count',
feedback_type = 'IMPLICIT'
) as
SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_South`;

create or replace model `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_West`options (model_type = 'matrix_factorization',
user_col = 'customer_id',
item_col = 'pdm_prod_type_id',
rating_col = 'trans_count',
feedback_type = 'IMPLICIT'
) as
SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_West`;

create or replace model `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_UNKNOWN`options (model_type = 'matrix_factorization',
user_col = 'customer_id',
item_col = 'pdm_prod_type_id',
rating_col = 'trans_count',
feedback_type = 'IMPLICIT'
) as
SELECT * FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_UNKNOWN`;


CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_Northeast` AS (
WITH predictions AS (
    SELECT 
      customer_id, 
      ARRAY_AGG(STRUCT(pdm_prod_type_id, 
                       predicted_trans_count_confidence)
                ORDER BY 
                  predicted_trans_count_confidence DESC
                ) as recommended
    FROM ML.RECOMMEND(MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_Northeast`)
    where  customer_id in (select row_number() over (order by PDM_PROD_TYPE_ID) 
    from (select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`) )
    GROUP BY customer_id
)
SELECT
  customer_id,
  pdm_prod_type_id,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
FROM
  predictions p,
  UNNEST(recommended)
);

CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_West` AS (
WITH predictions AS (
    SELECT 
      customer_id, 
      ARRAY_AGG(STRUCT(pdm_prod_type_id, 
                       predicted_trans_count_confidence)
                ORDER BY 
                  predicted_trans_count_confidence DESC
                ) as recommended
    FROM ML.RECOMMEND(MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_West`)
    where  customer_id in (select row_number() over (order by PDM_PROD_TYPE_ID) 
    from (select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`) )
    GROUP BY customer_id
)
SELECT
  customer_id,
  pdm_prod_type_id,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
FROM
  predictions p,
  UNNEST(recommended)
);


CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_South` AS (
WITH predictions AS (
    SELECT 
      customer_id, 
      ARRAY_AGG(STRUCT(pdm_prod_type_id, 
                       predicted_trans_count_confidence)
                ORDER BY 
                  predicted_trans_count_confidence DESC
                ) as recommended
    FROM ML.RECOMMEND(MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_South`)
    where  customer_id in (select row_number() over (order by PDM_PROD_TYPE_ID) 
    from (select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`) )
    GROUP BY customer_id
)
SELECT
  customer_id,
  pdm_prod_type_id,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
FROM
  predictions p,
  UNNEST(recommended)
);

CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_midwest` AS (
WITH predictions AS (
    SELECT 
      customer_id, 
      ARRAY_AGG(STRUCT(pdm_prod_type_id, 
                       predicted_trans_count_confidence)
                ORDER BY 
                  predicted_trans_count_confidence DESC
                ) as recommended
    FROM ML.RECOMMEND(MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_midwest`)
    where  customer_id in (select row_number() over (order by PDM_PROD_TYPE_ID) 
    from (select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`) )
    GROUP BY customer_id
)
SELECT
  customer_id,
  pdm_prod_type_id,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
FROM
  predictions p,
  UNNEST(recommended)
);

CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_UNKNOWN` AS (
WITH predictions AS (
    SELECT 
      customer_id, 
      ARRAY_AGG(STRUCT(pdm_prod_type_id, 
                       predicted_trans_count_confidence)
                ORDER BY 
                  predicted_trans_count_confidence DESC
                ) as recommended
    FROM ML.RECOMMEND(MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommender_UNKNOWN`)
    where  customer_id in (select row_number() over (order by PDM_PROD_TYPE_ID) 
    from (select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`) )
    GROUP BY customer_id
)
SELECT
  customer_id,
  pdm_prod_type_id,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
FROM
  predictions p,
  UNNEST(recommended)
);



CREATE OR REPLACE TABLE
  `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommendations_Midwest` AS
  SELECT 
  FOCUS_pdm_prod_type_id,
  focus_type,
  RECOMM_pdm_prod_type_id,
  recomm_type,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY FOCUS_pdm_prod_type_id ORDER BY predicted_trans_count_confidence DESC) AS rn
  FROM
  (
SELECT
  B.pdm_prod_type_id AS FOCUS_pdm_prod_type_id,
  d.PDM_PROD_TYPE_DESC as focus_type,
  A.pdm_prod_type_id AS RECOMM_pdm_prod_type_id,
  e.PDM_PROD_TYPE_DESC as recomm_type,
  predicted_trans_count_confidence,
  rn
FROM 
(
  select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
)a)B

JOIN

(
  SELECT
    *
  FROM (
    SELECT
      customer_id,
      pdm_prod_type_id,
      predicted_trans_count_confidence,
      ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
    FROM
     `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_midwest` )
  ORDER BY
    1,
    4 ) A
    ON
  B.customer_id = A.customer_id and b.pdm_prod_type_id!=a.pdm_prod_type_id
    inner join
    `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` d
on b.pdm_prod_type_id=d.PDM_PROD_TYPE_ID
inner join 
`dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` e
on a.pdm_prod_type_id=e.PDM_PROD_TYPE_ID


  ) z
  where rn<=50
ORDER BY
  1,
  6 ;

 
CREATE OR REPLACE TABLE
  `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommendations_South` AS
  SELECT 
  FOCUS_pdm_prod_type_id,
  focus_type,
  RECOMM_pdm_prod_type_id,
  recomm_type,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY FOCUS_pdm_prod_type_id ORDER BY predicted_trans_count_confidence DESC) AS rn
  FROM
  (
SELECT
  B.pdm_prod_type_id AS FOCUS_pdm_prod_type_id,
  d.PDM_PROD_TYPE_DESC as focus_type,
  A.pdm_prod_type_id AS RECOMM_pdm_prod_type_id,
  e.PDM_PROD_TYPE_DESC as recomm_type,
  predicted_trans_count_confidence,
  rn
FROM 
(
  select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
)a)B

JOIN

(
  SELECT
    *
  FROM (
    SELECT
      customer_id,
      pdm_prod_type_id,
      predicted_trans_count_confidence,
      ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
    FROM
     `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_South` )
  ORDER BY
    1,
    4 ) A
    ON
  B.customer_id = A.customer_id and b.pdm_prod_type_id!=a.pdm_prod_type_id
    inner join
    `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` d
on b.pdm_prod_type_id=d.PDM_PROD_TYPE_ID
inner join 
`dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` e
on a.pdm_prod_type_id=e.PDM_PROD_TYPE_ID
  ) z
  where rn<=50
ORDER BY
  1,
  6 ;


CREATE OR REPLACE TABLE
  `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommendations_UNKNOWN` AS
  SELECT 
  FOCUS_pdm_prod_type_id,
  focus_type,
  RECOMM_pdm_prod_type_id,
  recomm_type,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY FOCUS_pdm_prod_type_id ORDER BY predicted_trans_count_confidence DESC) AS rn
  FROM
  (
SELECT
  B.pdm_prod_type_id AS FOCUS_pdm_prod_type_id,
  d.PDM_PROD_TYPE_DESC as focus_type,
  A.pdm_prod_type_id AS RECOMM_pdm_prod_type_id,
  e.PDM_PROD_TYPE_DESC as recomm_type,
  predicted_trans_count_confidence,
  rn
FROM 
(
  select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
)a)B

JOIN

(
  SELECT
    *
  FROM (
    SELECT
      customer_id,
      pdm_prod_type_id,
      predicted_trans_count_confidence,
      ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
    FROM
     `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_UNKNOWN` )
  ORDER BY
    1,
    4 ) A
    ON
  B.customer_id = A.customer_id and b.pdm_prod_type_id!=a.pdm_prod_type_id
    inner join
    `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` d
on b.pdm_prod_type_id=d.PDM_PROD_TYPE_ID
inner join 
`dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` e
on a.pdm_prod_type_id=e.PDM_PROD_TYPE_ID
  ) z
  where rn<=50
ORDER BY
  1,
  6 ;


CREATE OR REPLACE TABLE
  `dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommendations_Northeast` AS
  SELECT 
  FOCUS_pdm_prod_type_id,
  focus_type,
  RECOMM_pdm_prod_type_id,
  recomm_type,
  predicted_trans_count_confidence,
  ROW_NUMBER() OVER(PARTITION BY FOCUS_pdm_prod_type_id ORDER BY predicted_trans_count_confidence DESC) AS rn
  FROM
  (
SELECT
  B.pdm_prod_type_id AS FOCUS_pdm_prod_type_id,
  d.PDM_PROD_TYPE_DESC as focus_type,
  A.pdm_prod_type_id AS RECOMM_pdm_prod_type_id,
  e.PDM_PROD_TYPE_DESC as recomm_type,
  predicted_trans_count_confidence,
  rn
FROM 
(
  select row_number() over (order by PDM_PROD_TYPE_ID) as customer_id, PDM_PROD_TYPE_ID, trans_count
from 
(
select distinct PDM_PROD_TYPE_ID, 1 as trans_count from `dw-bq-data-d00.SANDBOX_ANALYTICS.customer_base_table`
)a)B

JOIN

(
  SELECT
    *
  FROM (
    SELECT
      customer_id,
      pdm_prod_type_id,
      predicted_trans_count_confidence,
      ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY predicted_trans_count_confidence DESC) AS rn
    FROM
     `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendation_model_Northeast` )
  ORDER BY
    1,
    4 ) A
    ON
  B.customer_id = A.customer_id and b.pdm_prod_type_id!=a.pdm_prod_type_id
    inner join
    `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` d
on b.pdm_prod_type_id=d.PDM_PROD_TYPE_ID
inner join 
`dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE` e
on a.pdm_prod_type_id=e.PDM_PROD_TYPE_ID


  ) z
  where rn<=50
ORDER BY
  1,
  6 ;


CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendations00` as
SELECT
  f.FOCUS_pdm_prod_type_id,
  f.RECOMM_pdm_prod_type_id,
  f.predicted_trans_count_confidence,
  
from
`dw-bq-data-d00.SANDBOX_ANALYTICS.layer2_recommendations_prep00` f
left join
(SELECT 
     distinct a.item_sku_num, 
     a.pdm_prod_type_id, 
     a.item_sku_desc,
     pdm_prod_type_desc,  
     d.l1,d.l2,d.l3,d.l4,
     FROM `dw-bq-data-p00.COMM_MCF_TB.ITEM_SKU` A
     join `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE`  B
     on A.PDM_PROD_TYPE_ID = B.PDM_PROD_TYPE_ID
     join `dw-bq-data-d00.SANDBOX_ANALYTICS.sku_l1l2l3_all` D
     on A.ITEM_SKU_NUM=d.ITEM_SKU_NUM
     where A.PDM_PROD_TYPE_ID > 0
     and PDM_PROD_TYPE_DESC  != 'UNKNOWN'
    ) A
on f.FOCUS_item_sku_num=A.item_sku_num 
 LEFT JOIN 
    (SELECT 
     distinct a.item_sku_num, 
     a.pdm_prod_type_id, 
     a.item_sku_desc,
     pdm_prod_type_desc,  
     d.l1,d.l2,d.l3,d.l4,
     FROM `dw-bq-data-p00.COMM_MCF_TB.ITEM_SKU` A
     join `dw-bq-data-p00.EDW_MCF_VW.PDM_PROD_TYPE`  B
     on A.PDM_PROD_TYPE_ID = B.PDM_PROD_TYPE_ID
     join `dw-bq-data-d00.SANDBOX_ANALYTICS.sku_l1l2l3_all` D
     on A.ITEM_SKU_NUM=d.ITEM_SKU_NUM
     where A.PDM_PROD_TYPE_ID > 0
     and PDM_PROD_TYPE_DESC  != 'UNKNOWN'
    ) S 

    ON f.RECOMM_item_sku_num=S.item_sku_num 
    ORDER BY
  focus_sku_num,focus_sku,
  predicted_buy_flag_confidence desc;


select * from `dw-bq-data-d00.SANDBOX_ANALYTICS.l2recommendations00` where pdm_prod_type_id=2
ORDER BY
  focus_sku_num,focus_sku,
  predicted_buy_flag_confidence desc;
