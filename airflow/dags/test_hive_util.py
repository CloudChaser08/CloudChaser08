import util.emr_utils as emr_utils

sqls = [
    '''DROP TABLE IF EXISTS dev.temp_ref_nucc''',
    '''CREATE TABLE dev.temp_ref_nucc (
            code string,
            taxonomy_type string,
            classification string,
            specialization string,
            definition string,
            notes string,
            version string
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      STORED AS TEXTFILE
      LOCATION 's3a://salusv/testing/dewey/airflow/e2e/reference/nucc/'
    ''',
    '''CREATE TABLE IF NOT EXISTS dev.ref_nucc (
            code string,
            taxonomy_type string,
            classification string,
            specialization string,
            definition string,
            notes string,
            version string
      )
      STORED AS PARQUET 
      LOCATION 's3a://salusv/testing/dewey/airflow/e2e/reference/parquet/nucc/'
      ''',
      '''
      INSERT INTO dev.ref_nucc
      SELECT * FROM (
        SELECT * FROM dev.temp_ref_nucc
        UNION
        SELECT * FROM dev.ref_nucc
      ) a
      '''
]

emr_utils.run_hive_queries('hive_query_step_testing', sqls)
