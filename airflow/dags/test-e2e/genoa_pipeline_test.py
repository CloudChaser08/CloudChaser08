import subprocess

GENOA_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/genoa/pharmacyclaims'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', GENOA_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', GENOA_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', GENOA_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'genoa_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'genoa_pipeline',
        '-s', '2017-12-01T12:00:00',
        '-e', '2017-12-01T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', GENOA_TEST_DIR + '/out/2017/12/01/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', GENOA_TEST_DIR + '/payload/2017/12/01/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', GENOA_TEST_DIR + '/spark-output/part_provider=genoa/'
    ])) > 0


def test_cleanup():
    cleanup()
