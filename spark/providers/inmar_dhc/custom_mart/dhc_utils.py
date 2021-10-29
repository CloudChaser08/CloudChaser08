"""
Loads prod data for dedup
"""

import spark.helpers.s3_utils as s3_utils

def dhc_hist_record_loader(driver, path, date_input):

    if any(s3_utils.list_folders(path)):
        driver.spark.read.parquet(path).createOrReplaceTempView('_temp_rxtoken_nb')
    else:
        v_sql = "select cast(null as string) as claim_id, '{}' as part_file_date".format(date_input)
        driver.spark.sql(v_sql).createOrReplaceTempView('_temp_rxtoken_nb')
