import subprocess

EMDEON_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/emdeon/era'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', EMDEON_TEST_DIR + '/out/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'emdeon_era_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'emdeon_era_pipeline',
        '-s', '2017-06-28T12:00:00', '-e', '2017-06-28T12:00:00'
    ])


def test_raw_files_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EMDEON_TEST_DIR + '/out/2017/06/27/claims/'
    ])) > 0

    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EMDEON_TEST_DIR + '/out/2017/06/27/servicelines/'
    ])) > 0

    assert len(subprocess.check_output([
        'aws', 's3', 'ls', EMDEON_TEST_DIR + '/out/2017/06/27/link/'
    ])) > 0


def test_cleanup():
    cleanup()
