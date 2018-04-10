import subprocess

MCKESSON_MACRO_HELIX_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/mckesson_macro_helix'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', MCKESSON_MACRO_HELIX_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', MCKESSON_MACRO_HELIX_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', MCKESSON_MACRO_HELIX_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'mckesson_macro_helix_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'mckesson_macro_helix_pipeline',
        '-s', '2018-04-02T12:00:00',
        '-e', '2018-04-02T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', MCKESSON_MACRO_HELIX_TEST_DIR + '/out/2018/04/02/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', MCKESSON_MACRO_HELIX_TEST_DIR + '/payload/2018/04/02/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', MCKESSON_MACRO_HELIX_TEST_DIR + '/spark-output/part_provider=mckesson_macro_helix/'
    ])) > 0


def test_cleanup():
    cleanup()
