# create date table
from datetime import timedelta

LOCATION = 's3://healthveritydev/musifer/tmp/'


def generate(runner, start_date, end_date):

    date_range = [
        start_date + timedelta(n)
        for n in range(int((end_date - start_date).days))
    ]

    out_file = '/tmp/temp.csv'
    with open(out_file, 'w') as output:
        for single_date in date_range:
            output.write(
                single_date.strftime("%Y%m%d") + ',' +
                single_date.strftime("%Y-%m-%d") + '\n'
            )

    runner.run_spark_query('DROP TABLE IF EXISTS dates')
    runner.run_spark_query(
        'CREATE EXTERNAL TABLE dates (date string, formatted date) '
        + 'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' '
        + 'STORED AS TEXTFILE '
        + 'LOCATION \'' + LOCATION + '\''
    )
    runner.run_spark_query(
        """LOAD DATA LOCAL INPATH '""" + out_file + """' INTO TABLE dates"""
    )
