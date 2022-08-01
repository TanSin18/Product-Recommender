CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.lapsed_atrisk_clusters` as
SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `dw-bq-data-d00.SANDBOX_ANALYTICS.cluster_training_prod_type`,
    (
    SELECT
      *
    FROM
      `dw-bq-data-d00.SANDBOX_ANALYTICS.scored_cluster_data`
      
    ));
