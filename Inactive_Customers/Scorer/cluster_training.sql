CREATE OR REPLACE MODEL
  `dw-bq-data-d00.SANDBOX_ANALYTICS.cluster_training_prod_type`
OPTIONS
  ( MODEL_TYPE='KMEANS') AS
SELECT
  * EXCEPT(customer_id)
FROM `dw-bq-data-d00.SANDBOX_ANALYTICS.prod_type_cluster_data`;
