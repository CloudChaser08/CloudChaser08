import subprocess

NPPES_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/reference/nppes'

def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', NPPES_TEST_DIR
    ])

def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'reference_nppes'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'reference_nppes',
        '-s', '2018-03-01T12:00:00',
        '-e', '2018-03-01T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', NPPES_TEST_DIR + '/2018-03-01/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', NPPES_TEST_DIR + '/parquet/2018-03-01/'
    ])) > 0


def test_cleanup():
    cleanup()
