{{ config(
    materialized='incremental',
    unique_key=['reportDate','operatingSystem'],
    merge_update_columns = ['totalPageViews'],
    partition_by={
      "field": "reportDate",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = "operatingSystem"
)}}
SELECT 
    PARSE_TIMESTAMP("%Y%m%d",s.date) as reportDate,   
    device.operatingSystem as operatingSystem,
    sum(s.totals.pageviews) totalPageViews
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_*` s,
    unnest(s.hits) h  
GROUP BY
    s.date,   
    device.operatingSystem