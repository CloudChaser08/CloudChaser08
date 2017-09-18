import subprocess

NUCC_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/reference/nucc/'
NUCC_PARQUET_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/reference/parquet/nucc/'

def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', NUCC_TEST_DIR
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', NUCC_TEST_DIR
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'reference_nucc'
    ])

    subprocess.check_call([
        'airflow', 'backfill', 'reference_nucc',
        '-s', '2008-12-08',
        '-e', '2008-12-08',
        '-I'
    ])


def test_csv_file_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', NUCC_TEST_DIR + '2009-01-08'
    ])) > 0


def test_parquet_file_created():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', NUCC_PARQUET_TEST_DIR + '2009-01-08'
    ])) > 0


def test_cleanup():
    cleanup()


if __name__ == '__main__':
    # Run the DAG
    test_run()
    # Run tests
    test_csv_file_pushed()
    test_parquet_file_created()

