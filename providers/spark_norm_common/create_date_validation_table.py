# create date table
from datetime import timedelta

LOCATION = 's3://healthveritydev/musifer/tmp/'


def generate(runner, start_date, end_date):

    date_range = [
        start_date + timedelta(n)
        for n in range(int((end_date - start_date).days))
    ]

    with open('temp.csv', 'w') as output:
        for single_date in date_range:
            output.write(
                single_date.strftime("%Y%m%d") + ',' +
                single_date.strftime("%Y-%m-%d") + '\n'
            )

    runner.enqueue_psql_query('DROP TABLE IF EXISTS dates')
    runner.enqueue_psql_query(
        'CREATE EXTERNAL TABLE dates (date string, formatted date) '
        + 'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' '
        + 'STORED AS TEXTFILE '
        + 'LOCATION \'' + LOCATION + '\''
    )
    runner.enqueue_psql_query(
        'LOAD DATA INPATH \'temp.csv\' INTO TABLE dates'
    )
