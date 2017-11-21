import subprocess

APOTHECARY_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/apothecarybydesign/pharmacyclaims'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', APOTHECARY_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', APOTHECARY_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', APOTHECARY_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'apothecary_by_design_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'apothecary_by_design_pipeline',
        '-s', '2017-11-20T16:00:00',
        '-e', '2017-11-20T16:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', APOTHECARY_TEST_DIR + '/out/2017/11/27/transactions/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', APOTHECARY_TEST_DIR + '/payload/2017/11/27/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', APOTHECARY_TEST_DIR + '/spark-output/part_provider=apothecarybydesign/'
    ])) > 0


def test_cleanup():
    cleanup()
