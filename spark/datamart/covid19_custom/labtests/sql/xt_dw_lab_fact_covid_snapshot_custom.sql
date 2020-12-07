CREATE EXTERNAL TABLE IF NOT EXISTS dw.lab_fact_covid_snapshot_custom
(
    claim_id	                string
    ,hvid	                    string
    ,date_service	            date
    ,part_provider	            string
    ,hv_test_flag	            string
    ,hv_result_flag	            string
    ,result	                    string
    ,result_comments	        string
    ,claim_bucket_id            int
    ,myrow                      int
)
STORED AS PARQUET
LOCATION {table_location}