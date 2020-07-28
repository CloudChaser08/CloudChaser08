CREATE EXTERNAL TABLE IF NOT EXISTS dw.lab_ref_covid
(
    part_provider	            string
    ,test_ordered_name	        string
    ,result_name	            string
    ,result	                    string
    ,result_comments	        string
    ,hv_method_flag	            int
    ,hv_test_flag	            string
    ,hv_result_flag	            string
)
STORED AS PARQUET
LOCATION {table_location}


