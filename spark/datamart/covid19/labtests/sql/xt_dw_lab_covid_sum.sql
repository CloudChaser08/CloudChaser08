CREATE EXTERNAL TABLE IF NOT EXISTS dw.lab_covid_sum
(
    week_end	        date
    ,hv_test_flag	    string
    ,hv_result_flag	    string
    ,supplier	        string
    ,tests	            string
    ,pats	            string
)
STORED AS PARQUET
LOCATION {table_location}


