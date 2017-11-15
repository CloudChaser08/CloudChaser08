import subprocess

CARDINAL_PDS_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims'

def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_PDS_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_PDS_TEST_DIR + '/normalized/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_PDS_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'cardinal_pds_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'cardinal_pds_pipeline',
        '-s', '2017-10-27', '-e', '2017-10-27', '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_PDS_TEST_DIR + '/out/2017/08/28/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_PDS_TEST_DIR + '/normalized/'
    ])) > 0


def test_incoming_files_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_PDS_TEST_DIR + '/moved_raw/'
    ])) > 0
