import subprocess

EXPRESS_SCRIPTS_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', EXPRESS_SCRIPTS_TEST_DIR + '/incoming/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', EXPRESS_SCRIPTS_TEST_DIR + '/matching/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', EXPRESS_SCRIPTS_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'express_scripts_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'express_scripts_pipeline',
        '-s', '2017-07-17T00:00:00',
        '-e', '2017-07-17T00:00:00'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EXPRESS_SCRIPTS_TEST_DIR + '/incoming/2017/07/23/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EXPRESS_SCRIPTS_TEST_DIR + '/matching/2017/07/23/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EXPRESS_SCRIPTS_TEST_DIR + '/spark-output/part_provider=express_scripts/'
    ])) > 0


def test_cleanup():
    cleanup()
