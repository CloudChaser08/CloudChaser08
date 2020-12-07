"""
Covid Custom Labtests Context
"""


"""
Datamart and Location Details
"""
DATA_TYP = 'DATAMART'
DATAMART_NAME = 'LAB-COVID19-CUSTOM'
REFRESH_IND = 'D'
DATAMART_SHORT_NOTES = 'Lab-Covid-Custom Fact, Ref. and Summary -tests and results'

# Final s3 output Path
PRODUCTION = 's3://salusv/warehouse/'
TRANSFORM = 's3://salusv/warehouse/transformed/'
LAB_DATAMART_PATH = 'datamart/covid19_custom/lab/'
LAB_DATAMART_STAGE_PATH = 'datamart/covid19_custom/lab/_stage/'
LAB_DATAMART_ARCHIVE_PATH = 'datamart/archive/covid19_custom/lab/'

MDATA_TABLE_LOCATION = 's3://salusv/warehouse/parquet/mdm/mdata/'
MDATA_TABLE = 'mdata'
MDATA_VIEW = 'v_mdata'

"""
HDFS Temp Location
"""
HDFS_OUTPUT_PATH = '/staging/'


"""
Datamart Configuration for...........LabTests
"""
LAB_PART_PROVIDER_LIST = ['quest', 'bioreference', 'ovation', 'luminate', 'labcorp_covid']
LAB_PARTITIONS = ['part_mth', 'part_provider']
LAB_FIRST_RUN_MONTH = '2018-01'
LAB_REFRESH_NBR_OF_MONTHS = 6
LAB_SKIP_ARCHIVE = False

FULL_ARCHIVE_REQUESTED_DAYS = ['Sunday']
FULL_ARCHIVE_SUFFIX_KEY = 'F'
INCREMENTAL_ARCHIVE_SUFFIX_KEY = 'I'
LAB_BYPASS_STAGE_TRANS_HDFS_TO_PROD = False  # do not enable this option for incremental; risk for data duplicates

"""
Identify providers to balance 
    number of partitions and read data from S3
"""
LAB_BIG_PART_PROVIDER = ['quest']
LAB_MEDIUM_PART_PROVIDER = ['bioreference', 'labcorp_covid']
LAB_SMALL_PART_PROVIDER = ['ovation', 'luminate']


"""
Source Tables Lab Current Product Table Details
"""
DW_SCHEMA = 'dw'
LAB_DW_SCHEMA = 'dw'

"""
Lab Data Warehouse Path
"""
LAB_WAREHOUSE_PATH = 's3a://salusv/warehouse/parquet/labtests/2017-02-16/'


""" 
Source 1
"""
LAB_DW_DB_TABLE_NAME = 'dw._labtests_nb'
LAB_DW_TABLE_NAME = '_labtests_nb'
LAB_DW_TABLE_IS_PARTITIONED = True

"""
Source 2
"""
LAB_RESULTS_SCHEMA = 'aet2575'
LAB_RESULTS_TABLE_NAME = 'hvrequest_output_002575'
LAB_RESULTS_DB_TABLE_NAME = 'aet2575.hvrequest_output_002575'
LAB_RESULTS_TABLE_IS_PARTITIONED = True

"""
Final Target Tables 
"""

"""Intermediate table also Covid Target"""
LAB_DW_COVID_CLEANSED_TABLE_NAME_CUSTOM = 'lab_fact_covid_cleansed_custom'
LAB_DW_COVID_CLEANSED_DB_TABLE_NAME_CUSTOM = 'dw.lab_fact_covid_cleansed_custom'
LAB_DW_COVID_CLEANSED_TABLE_IS_PARTITIONED = True


"""
Final Delivery Table Names 
    and S3 Location Name
"""
LAB_FACT_ALL_TESTS = 'lab_fact_all_tests'
LAB_FACT_ALL_TESTS_CUSTOM = 'lab_fact_all_tests_custom'
LAB_FACT_COVID_TESTS_CUSTOM = 'lab_fact_covid_tests_custom'
LAB_FACT_COVID_CLEANSED_CUSTOM = 'lab_fact_covid_cleansed_custom'
LAB_REF_COVID_CUSTOM = 'lab_ref_covid_custom'
LAB_COVID_SNAPSHOT_CUSTOM = 'lab_fact_covid_snapshot_custom'
LAB_COVID_SUM_CUSTOM = 'lab_covid_sum_custom'

"""
Added below List for Operational Purpose
..to extract data from source (provider level extract)
"""

LAB_TABLES_LIST_CUSTOM = [LAB_FACT_ALL_TESTS,
                          LAB_FACT_ALL_TESTS_CUSTOM,
                          LAB_FACT_COVID_TESTS_CUSTOM,
                          LAB_FACT_COVID_CLEANSED_CUSTOM,
                          LAB_REF_COVID_CUSTOM,
                          LAB_COVID_SNAPSHOT_CUSTOM,
                          LAB_COVID_SUM_CUSTOM]

"""
List of Partitioned Target Tabels
"""
LAB_PARTTITIONED_TABLES_LIST_CUSTOM = [LAB_FACT_ALL_TESTS,
                                       LAB_FACT_ALL_TESTS_CUSTOM,
                                       LAB_FACT_COVID_TESTS_CUSTOM,
                                       LAB_FACT_COVID_CLEANSED_CUSTOM]



"""
DATAMART Operational Configuration for...........LabTests
"""
LAB_NBR_OF_BUCKETS = 100
PART_MTH_PATTERN = 'part_mth='
PART_PROVIDER_PATTERN = 'part_provider='
FILE_NAME_PATTERN = '.gz.parquet'

NUMBER_OF_MONTHS_PER_EXTRACT = 7
NUMBER_OF_MONTHS_PER_EXTRACT_IN_HDFS = 7


"""
S3 Location:

s3a://salusv/warehouse/
                    datamart/covid19_custom/lab/
                        lab_fact_all_tests/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_fact_all_tests_custom/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_fact_covid_tests_custom/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_fact_covid_cleansed_custom/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_ref_covid_custom/
                        lab_fact_covid_snapshot_custom/
                        lab_covid_sum_custom/
"""

