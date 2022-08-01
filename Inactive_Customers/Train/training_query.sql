create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.reengagement_product_recommendation_training` as 
WITH R_CUST_PRDS AS (
  SELECT 
    a.customer_guid, 
    q.address_id, 
    a.transaction_guid, 
    a.transaction_booked_date, 
    f.point_of_sale_item_sku_num, 
    s.pdm_prod_type_id, 
    s.item_sku_desc, 
    s.pdm_prod_type_desc 
  from 
    --Reactivated Customers
    (
      SELECT 
        customer_guid, 
        transaction_guid, 
        transaction_booked_date, 
        concept_format_id, 
        segment_for_BBBY_usa_and_all_channels 
      FROM 
        `dw-bq-data-p00.ANALYTICAL.new_engaged_active_reactivated_customer` 
      WHERE 
        transaction_booked_date between date_sub(
          current_date(), 
          interval 1 YEAR
        ) 
        and current_date() 
        AND segment_for_BBBY_usa_and_all_channels = 'R' 
        AND concept_format_id = 1
    ) a 
    INNER JOIN --Reactivation Purchase Transaction Detail 
    (
      SELECT 
        transaction_guid, 
        point_of_sale_item_sku_num, 
        transaction_booked_date, 
        concept_format_id, 
        sold_units 
      FROM 
        `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_line_sum` 
      where 
        transaction_booked_date between date_sub(
          current_date(), 
          interval 1 YEAR
        ) 
        and current_date() 
        AND concept_format_id = 1
    ) f ON a.transaction_guid = f.transaction_guid 
    AND a.transaction_booked_date = f.transaction_booked_date --SKU Related Info 
    LEFT JOIN (
      SELECT 
        distinct item_sku_num, 
        pdm_prod_type_id, 
        item_sku_desc, 
        pdm_prod_type_desc, 
      FROM 
        `dw-bq-data-d00.SANDBOX_ANALYTICS.sales_transaction_distinct_skus`
    ) S ON f.point_of_sale_item_sku_num = S.item_sku_num --Address ID 
    LEFT JOIN (
      SELECT 
        customer_id, 
        address_id 
      FROM 
        `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_CURR` 
      WHERE 
        customer_purge_ind = 'N' 
        AND customer_id > 0 
        AND address_id > 0
    ) Q ON a.customer_guid = Q.customer_id
), 
date_diff_tss AS (
  select 
    customer_id, 
    transaction_guid, 
    transaction_booked_date as reactivation_purchase, 
    date_trunc(transaction_booked_date, month) as reactivation_month, 
    LAG(transaction_booked_date) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        transaction_booked_date
    ) as previous_buy, 
    CAST(
      concat(
        cast(
          FORMAT_DATE(
            "%Y", 
            DATE_SUB(
              LAG(transaction_booked_date) OVER (
                PARTITION BY customer_id 
                ORDER BY 
                  transaction_booked_date
              ), 
              INTERVAL 3 MONTH
            )
          ) AS String
        ), 
        cast(
          DATE_SUB(
            LAG(transaction_booked_date) OVER (
              PARTITION BY customer_id 
              ORDER BY 
                transaction_booked_date
            ), 
            INTERVAL 3 MONTH
          ) as string format('MM')
        )
      ) as int
    ) as FP, 
    CAST(
      concat(
        cast(
          FORMAT_DATE(
            "%Y", 
            DATE_SUB(
              transaction_booked_date, INTERVAL 3 MONTH
            )
          ) AS String
        ), 
        cast(
          DATE_SUB(
            transaction_booked_date, INTERVAL 3 MONTH
          ) as string format('MM')
        )
      ) as int
    ) as FP2, 
    ABS(
      date_diff(
        LAG(transaction_booked_date) OVER (
          PARTITION BY customer_id 
          ORDER BY 
            transaction_booked_date
        ), 
        transaction_booked_date, 
        day
      )
    ) as time_interval 
  FROM 
    (
      SELECT 
        customer_id, 
        transaction_booked_date, 
        transaction_guid 
      FROM 
        `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_sum` a 
        JOIN `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_TXN_XREF` b ON a.transaction_guid = b.trans_id 
        AND b.customer_id > 0 
      WHERE 
        concept_format_id = 1 
        AND customer_id in (
          select 
            customer_guid 
          from 
            R_CUST_PRDS 
          group by 
            1
        ) 
        AND gross_sales is not null 
        AND (
          SALES_TRANSACTION_TYPE_CODE = 'S' 
          or SALES_TRANSACTION_TYPE_CODE = 'M'
        ) 
      group by 
        1, 
        2, 
        3
    ) a
), 
base AS (
  SELECT 
    a.customer_guid AS customer_id, 
    a.address_id, 
    b.reactivation_purchase, 
    b.previous_buy, 
    b.FP, 
    b.FP2, 
    b.time_interval, 
    a.pdm_prod_type_id, 
    a.pdm_prod_type_desc 
  FROM 
    R_CUST_PRDS a 
    join date_diff_tss b ON a.customer_guid = b.customer_id 
    AND a.transaction_guid = b.transaction_guid 
  WHERE 
    previous_buy >= '2017-01-01' 
    AND b.time_interval != 0 
    AND a.pdm_prod_type_id in (
      Select 
        pdm_prod_type_id 
      from 
        (
          select 
            distinct pdm_prod_type_id, 
            count(customer_guid) 
          from 
            R_CUST_PRDS 
          group by 
            1 
          order by 
            2 desc 
          limit 
            100
        )
    ) 
    AND time_interval between 365 
    and 720 
  group by 
    1,2,3,4,5,6,7,8,9
) 
select 
  base.*, 
  f.beyond_recency_two_years AS BBB_R_2Y, 
  f.beyond_instore_monetary_decile_two_years AS BBB_INSTORE_M_DECILE_2Y, 
  j.A_A7478N_NEW_MOVER_YES, 
  j.A_A8588N_HM_SQR_FT, 
  j.A_A3101N_RACE_WHITE, 
  j.A_AAP000447N_ASET_PRPN_DIS_INC, 
  g.COUPON_SALES_Q_08, 
  d.PH_DM_RECENCY, 
  d.PH_MREDEEM730D_PERC, 
  avg_net_sales_per_transaction AS AVG_NET_SALES_PER_TXN, 
  avg_items_per_transaction AS AVG_TOTAL_ITEMS_PER_TXN, 
  number_of_merchandise_divisions_purchased AS NUM_MERCH_DIVISIONS, 
  IFNULL(
    MAX(MOVER), 
    0
  ) as MOVER 
from 
  base base 
  LEFT JOIN (
    SELECT 
      address_guid, 
      fiscal_period_id, 
      A_A7478N_NEW_MOVER_YES, 
      A_A8588N_HM_SQR_FT, 
      A_A3101N_RACE_WHITE, 
      A_AAP000447N_ASET_PRPN_DIS_INC 
    FROM 
      `dw-bq-data-p00.ANALYTICAL.model_factory_address_demographics_history`
  ) j on base.address_id = j.address_guid 
  and base.FP2 = j.fiscal_period_id 
  LEFT JOIN (
    SELECT 
      fiscal_period_id, 
      address_guid, 
      beyond_recency_two_years, 
      beyond_instore_monetary_decile_two_years 
    FROM 
      `dw-bq-data-p00.ANALYTICAL.model_factory_address_rfm`
  ) f on base.address_id = f.address_guid 
  and base.FP = f.fiscal_period_id 
  LEFT JOIN (
    SELECT 
      fiscal_period_id, 
      address_guid, 
      PH_DM_RECENCY, 
      PH_MREDEEM730D_PERC 
    FROM 
      `dw-bq-data-p00.ANALYTICAL.model_factory_address_promo_history_variables`
  ) d on base.address_id = d.address_guid 
  and base.FP2 = d.fiscal_period_id 
  LEFT JOIN (
    SELECT 
      fiscal_period_id, 
      address_guid, 
      COUPON_SALES_Q_08 
    FROM 
      `dw-bq-data-p00.ANALYTICAL.model_factory_address_seasonality`
  ) g on base.address_id = g.address_guid 
  and base.FP = g.fiscal_period_id 
  LEFT JOIN (
    SELECT 
      fiscal_period_id, 
      address_guid, 
      avg_items_per_transaction, 
      avg_net_sales_per_transaction, 
      number_of_merchandise_divisions_purchased 
    FROM 
      `dw-bq-data-p00.ANALYTICAL.model_factory_shopping_metrics_history`
  ) s on base.address_id = s.address_guid 
  and base.FP = s.fiscal_period_id 
  LEFT JOIN (
    select 
      customer_guid, 
      lifestage_event, 
      lifestage_date, 
      case when lifestage_event = "WEDDING" then 1 else 0 end as WEDDING, 
      case when lifestage_event = "NEW MOVER" then 1 else 0 end as MOVER, 
      case when lifestage_event = "BABY" then 1 else 0 end as BABY, 
      case when lifestage_event = "RETIREMENT" then 1 else 0 end as RETIREMENT, 
      case when lifestage_event = "WEDDING" then DATE_SUB(lifestage_date, INTERVAL 547 DAY) when lifestage_event = "NEW MOVER" then DATE_SUB(lifestage_date, INTERVAL 60 DAY) when lifestage_event = "BABY" then DATE_SUB(lifestage_date, INTERVAL 90 DAY) when lifestage_event = "RETIREMENT" then DATE_SUB(lifestage_date, INTERVAL 304 DAY) end as Start_date, 
      case when lifestage_event = "WEDDING" then DATE_ADD(lifestage_date, INTERVAL 365 DAY) when lifestage_event = "NEW MOVER" then DATE_ADD(lifestage_date, INTERVAL 300 DAY) when lifestage_event = "BABY" then DATE_ADD(lifestage_date, INTERVAL 1095 DAY) when lifestage_event = "RETIREMENT" then DATE_ADD(lifestage_date, INTERVAL 1367 DAY) end as End_date 
    from 
      (
        SELECT 
          s.customer_guid, 
          s.lifestage_date, 
          t.lifestage_event 
        FROM 
          (
            SELECT 
              lifestage_date, 
              customer_guid, 
              lifestage_event 
            FROM 
              `dw-bq-data-p00.ANALYTICAL.life_stage_history`
          ) t 
          INNER JOIN (
            SELECT 
              customer_guid, 
              max(lifestage_date) as lifestage_date 
            FROM 
              `dw-bq-data-p00.ANALYTICAL.life_stage_history` 
            group by 
              1
          ) s on s.customer_guid = t.customer_guid 
          and s.lifestage_date = t.lifestage_date
      ) R
  ) W on base.customer_id = W.customer_guid 
  and DATE_DIFF(
    base.reactivation_purchase, Start_date, 
    DAY
  )>= 0 
  and DATE_DIFF(
    base.reactivation_purchase, End_date, 
    DAY
  )<= 0 
group by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21;
