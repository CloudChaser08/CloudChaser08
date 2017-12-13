import subprocess

HEALTHJUMP_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr'


def cleanup():
    # cleanup
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', HEALTHJUMP_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', HEALTHJUMP_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', HEALTHJUMP_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'healthjump_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'healthjump_pipeline',
        '-s', '2017-10-18T12:00:00',
        '-e', '2017-10-18T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', HEALTHJUMP_TEST_DIR + '/out/2017/10/18/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', HEALTHJUMP_TEST_DIR + '/payload/2017/10/18/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', HEALTHJUMP_TEST_DIR + '/spark-output/part_hvm_vdr_feed_id=47/'
    ])) > 0


def test_cleanup():
    cleanup()
