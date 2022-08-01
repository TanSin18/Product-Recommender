create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.reengagement_product_recommendation_scoring` as 
WITH customer_list as (
  select 
    a.*, 
    b.address_id 
  from 
    (
      select 
        customer_id, 
        max(transaction_booked_date) as latest_purchase, 
        CAST(
          concat(
            cast(
              FORMAT_DATE(
                "%Y", 
                DATE_SUB(
                  current_date(), 
                  INTERVAL 3 MONTH
                )
              ) AS String
            ), 
            cast(
              DATE_SUB(
                current_date(), 
                INTERVAL 3 MONTH
              ) as string format('MM')
            )
          ) as int
        ) as FP, 
        ABS(
          date_diff(
            current_date(), 
            max(transaction_booked_date), 
            day
          )
        ) as time_interval 
      from 
        (
          select 
            customer_id, 
            transaction_booked_date, 
            transaction_guid 
          from 
            `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_sum` a 
            join `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_TXN_XREF` b on a.transaction_guid = b.trans_id 
            and b.customer_id > 0 
          where 
            concept_format_id = 1 
            and customer_id in (
              select 
                distinct customer_id 
              from 
                `dw-bq-data-p00.SANDBOX_DA.dvh_AT_RISK_LAPSED_AUG_PC_2022`
            ) 
            and gross_sales is not null 
            and (
              SALES_TRANSACTION_TYPE_CODE = 'S' 
              or SALES_TRANSACTION_TYPE_CODE = 'M'
            ) 
          group by 
            1, 
            2, 
            3
        ) a 
      group by 
        1
    ) a 
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
    ) b ON a.customer_id = b.customer_id
) 
select 
  base.*, 
  f.beyond_recency_two_years AS BBB_R_2Y, 
  j.A_A7478N_NEW_MOVER_YES, 
  j.A_A8588N_HM_SQR_FT, 
  j.A_A3101N_RACE_WHITE, 
  j.A_AAP000447N_ASET_PRPN_DIS_INC, 
  g.COUPON_SALES_Q_08, 
  d.PH_DM_RECENCY, 
  f.beyond_instore_monetary_decile_two_years AS BBB_INSTORE_M_DECILE_2Y, 
  avg_net_sales_per_transaction AS AVG_NET_SALES_PER_TXN, 
  avg_items_per_transaction AS AVG_TOTAL_ITEMS_PER_TXN, 
  h.number_of_merchandise_divisions_purchased AS NUM_MERCH_DIVISIONS, 
  d.PH_MREDEEM730D_PERC, 
  IFNULL(
    MAX(MOVER), 
    0
  ) as MOVER 
from 
  customer_list base 
  LEFT JOIN (
    SELECT 
      s.address_guid, 
      s.fiscal_period_id, 
      t.A_A7478N_NEW_MOVER_YES, 
      t.A_A8588N_HM_SQR_FT, 
      t.A_A3101N_RACE_WHITE, 
      t.A_AAP000447N_ASET_PRPN_DIS_INC 
    FROM 
      (
        SELECT 
          fiscal_period_id, 
          address_guid, 
          A_A7478N_NEW_MOVER_YES, 
          A_A8588N_HM_SQR_FT, 
          A_A3101N_RACE_WHITE, 
          A_AAP000447N_ASET_PRPN_DIS_INC 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_demographics_history`
      ) t 
      INNER JOIN (
        SELECT 
          address_guid, 
          max(fiscal_period_id) as fiscal_period_id 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_demographics_history` 
        group by 
          1
      ) s on s.address_guid = t.address_guid 
      and s.fiscal_period_id = t.fiscal_period_id
  ) j on base.address_id = j.address_guid 
  LEFT JOIN (
    SELECT 
      s.address_guid, 
      s.fiscal_period_id, 
      t.beyond_recency_two_years, 
      t.beyond_instore_monetary_decile_two_years 
    FROM 
      (
        SELECT 
          fiscal_period_id, 
          address_guid, 
          beyond_recency_two_years, 
          beyond_instore_monetary_decile_two_years 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_rfm`
      ) t 
      INNER JOIN (
        SELECT 
          address_guid, 
          max(fiscal_period_id) as fiscal_period_id 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_rfm` 
        group by 
          1
      ) s on s.address_guid = t.address_guid 
      and s.fiscal_period_id = t.fiscal_period_id
  ) f on base.address_id = f.address_guid 
  LEFT JOIN (
    SELECT 
      s.address_guid, 
      s.fiscal_period_id, 
      t.PH_DM_RECENCY, 
      t.PH_MREDEEM730D_PERC 
    FROM 
      (
        SELECT 
          fiscal_period_id, 
          address_guid, 
          PH_DM_RECENCY, 
          PH_MREDEEM730D_PERC 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_promo_history_variables`
      ) t 
      INNER JOIN (
        SELECT 
          address_guid, 
          max(fiscal_period_id) as fiscal_period_id 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_promo_history_variables` 
        group by 
          1
      ) s on s.address_guid = t.address_guid 
      and s.fiscal_period_id = t.fiscal_period_id
  ) d on base.address_id = d.address_guid 
  LEFT JOIN (
    SELECT 
      s.address_guid, 
      s.fiscal_period_id, 
      t.COUPON_SALES_Q_08 
    FROM 
      (
        SELECT 
          fiscal_period_id, 
          address_guid, 
          COUPON_SALES_Q_08 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_seasonality`
      ) t 
      INNER JOIN (
        SELECT 
          address_guid, 
          max(fiscal_period_id) as fiscal_period_id 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_address_seasonality` 
        group by 
          1
      ) s on s.address_guid = t.address_guid 
      and s.fiscal_period_id = t.fiscal_period_id
  ) g on base.address_id = g.address_guid 
  LEFT JOIN (
    SELECT 
      s.address_guid, 
      s.fiscal_period_id, 
      t.avg_net_sales_per_transaction, 
      t.avg_items_per_transaction, 
      t.number_of_merchandise_divisions_purchased 
    FROM 
      (
        SELECT 
          fiscal_period_id, 
          address_guid, 
          avg_items_per_transaction, 
          avg_net_sales_per_transaction, 
          number_of_merchandise_divisions_purchased 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_shopping_metrics_history`
      ) t 
      INNER JOIN (
        SELECT 
          address_guid, 
          max(fiscal_period_id) as fiscal_period_id 
        FROM 
          `dw-bq-data-p00.ANALYTICAL.model_factory_shopping_metrics_history` 
        group by 
          1
      ) s on s.address_guid = t.address_guid 
      and s.fiscal_period_id = t.fiscal_period_id
  ) h on base.address_id = h.address_guid 
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
  ) W on base.CUSTOMER_ID = W.customer_guid 
  and DATE_DIFF(
    current_date(), 
    Start_date, 
    DAY
  )>= 0 
  and DATE_DIFF(
    current_date(), 
    End_date, 
    DAY
  )<= 0 
group by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;
