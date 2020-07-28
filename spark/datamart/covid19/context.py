"""
Covid Labtests Context
"""


"""
Datamart and Location Details
"""
DATA_TYP = 'DATAMART'
DATAMART_NAME = 'LAB-COVID19'
REFRESH_IND = 'D'
DATAMART_SHORT_NOTES = 'Lab-Covid Fact, Ref. and Summary -tests and results'

# Final s3 output Path
PRODUCTION = 's3://salusv/warehouse/'
TRANSFORM = 's3://salusv/warehouse/transformed/'
LAB_DATAMART_PATH = 'datamart/covid19/lab/'
LAB_DATAMART_STAGE_PATH = 'datamart/covid19/lab/_stage/'
LAB_DATAMART_ARCHIVE_PATH = 'datamart/archive/covid19/lab/'

MDATA_TABLE_LOCATION = 's3://salusv/warehouse/parquet/mdm/mdata/'
MDATA_TABLE = 'mdata'
MDATA_VIEW = 'v_mdata'

"""
HDFS Temp Location
"""
HDFS_OUTPUT_PATH = 'hdfs:///staging/'


"""
Datamart Configuration for...........LabTests
"""
LAB_PART_PROVIDER_LIST = ['quest', 'bioreference', 'ovation', 'luminate']
LAB_PARTITIONS = ['part_mth', 'part_provider']
LAB_FIRST_RUN_MONTH = '2018-01'
LAB_REFRESH_NBR_OF_MONTHS = 5
LAB_SKIP_ARCHIVE = False

FULL_ARCHIVE_REQUESTED_DAYS = ['Sunday']
FULL_ARCHIVE_SUFFIX_KEY = 'F'
INCREMENTAL_ARCHIVE_SUFFIX_KEY = 'I'


"""
Identify providers to balance 
    number of partitions and read data from S3
"""
LAB_BIG_PART_PROVIDER = ['quest']
LAB_MEDIUM_PART_PROVIDER = ['bioreference']
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
LAB_DW_COVID_CLEANSED_TABLE_NAME = 'lab_fact_covid_cleansed'
LAB_DW_COVID_CLEANSED_DB_TABLE_NAME = 'dw.lab_fact_covid_cleansed'
LAB_DW_COVID_CLEANSED_TABLE_IS_PARTITIONED = True


"""
Final Delivery Table Names 
    and S3 Location Name
"""
LAB_FACT_ALL_TESTS = 'lab_fact_all_tests'
LAB_FACT_COVID_TESTS = 'lab_fact_covid_tests'
LAB_FACT_COVID_CLEANSED = 'lab_fact_covid_cleansed'
LAB_REF_COVID = 'lab_ref_covid'
LAB_COVID_SNAPSHOT = 'lab_fact_covid_snapshot'
LAB_COVID_SUM = 'lab_covid_sum'


"""
Added below List for Operational Purpose
..to extract data from source (provider level extract)
"""

LAB_TABLES_LIST = [LAB_FACT_ALL_TESTS,
                   LAB_FACT_COVID_TESTS,
                   LAB_FACT_COVID_CLEANSED,
                   LAB_REF_COVID,
                   LAB_COVID_SNAPSHOT,
                   LAB_COVID_SUM]

"""
List of Partitioned Target Tabels
"""
LAB_PARTTITIONED_TABLES_LIST = [LAB_FACT_ALL_TESTS,
                                LAB_FACT_COVID_TESTS,
                                LAB_FACT_COVID_CLEANSED]




"""
DATAMART Operational Configuration for...........LabTests
"""
LAB_NBR_OF_BUCKETS = 20
NUMBER_OF_PAETITIONS = 100
PART_MTH_PATTERN = 'part_mth='
PART_PROVIDER_PATTERN = 'part_provider='
FILE_NAME_PATTERN = '.gz.parquet'

NUMBER_OF_MONTHS_PER_EXTRACT = 6
NUMBER_OF_MONTHS_PER_EXTRACT_IN_HDFS = 6


"""
S3 Location:

s3a://salusv/warehouse/
                    datamart/covid19/lab/
                        lab_fact_all_tests/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_fact_covid_tests/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_fact_covid_cleansed/part_mth=<part_mth>/part_provider=<part_provider>/
                        lab_ref_covid/
                        lab_fact_covid_snapshot/
                        lab_covid_sum/
"""

