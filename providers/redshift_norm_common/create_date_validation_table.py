# create date table
import subprocess
from datetime import timedelta, date, datetime


def generate(psql, db, creds):

    subprocess.call(' '.join(
        psql + [db, '-c', '"DROP TABLE IF EXISTS dates"']
    ), shell=True)
    subprocess.call(' '.join(
        psql + [db, '-c', '"CREATE TABLE dates (date text encode lzo, formatted text encode lzo) DISTSTYLE ALL"']
    ), shell=True)

    start_date = date(2012, 1, 1)
    end_date = datetime.now().date()
    date_range = [start_date + timedelta(n) for n in range(int((end_date - start_date).days))]

    with open('temp.csv', 'w') as output:
        for single_date in date_range:
            output.write(single_date.strftime("%Y%m%d") + ',' + single_date.strftime("%Y-%m-%d") + '\n')

    subprocess.call('aws s3 cp temp.csv s3://healthveritydev/musifer/tmp/', shell=True)

    subprocess.call(' '.join(
        psql + [db, '-c', '"COPY dates FROM \'s3://healthveritydev/musifer/tmp/temp.csv\' CREDENTIALS \''
                + creds + '\' FORMAT AS CSV;"']
    ), shell=True)


