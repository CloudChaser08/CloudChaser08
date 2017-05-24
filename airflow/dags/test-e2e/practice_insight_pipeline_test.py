import subprocess

PRACTICE_INSIGHT_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', PRACTICE_INSIGHT_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', PRACTICE_INSIGHT_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', PRACTICE_INSIGHT_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'practice_insight_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'practice_insight_pipeline',
        '-s', '2017-03-02T12:00:00',
        '-e', '2017-03-02T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', PRACTICE_INSIGHT_TEST_DIR + '/out/2017/03/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', PRACTICE_INSIGHT_TEST_DIR + '/payload/2017/03/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', PRACTICE_INSIGHT_TEST_DIR + '/spark-output/part_provider=practice_insight/'
    ])) > 0


def test_cleanup():
    cleanup()
