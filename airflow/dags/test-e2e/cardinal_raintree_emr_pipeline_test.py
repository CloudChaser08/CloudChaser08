import subprocess

CARDINAL_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_TEST_DIR + '/spark-output/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_TEST_DIR + '/deliverable/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_TEST_DIR + '/incoming/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'cardinal_raintree_emr_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'cardinal_raintree_emr_pipeline',
        '-s', '2017-12-26T16:00:00',
        '-e', '2017-12-26T16:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/out/2017/12/27/'
    ])) > 0
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/incoming/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/spark-output/clinical_observation/part_hvm_vdr_feed_id=40/'
    ])) > 0
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/spark-output/diagnosis/part_hvm_vdr_feed_id=40/'
    ])) > 0
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/spark-output/encounter/part_hvm_vdr_feed_id=40/'
    ])) > 0
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_TEST_DIR + '/spark-output/procedure/part_hvm_vdr_feed_id=40/'
    ])) > 0


def test_cleanup():
    cleanup()
