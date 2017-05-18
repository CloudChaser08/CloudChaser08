import subprocess

CARIS_TEST_DIR = 's3://healthveritydev/musifer/tests/airflow/caris'


def test_run():
    subprocess.check_call([
        'airflow', 'clear', '-c', 'caris_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'caris_pipeline',
        '-s', '2017-04-02T12:00:00',
        '-e', '2017-04-02T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARIS_TEST_DIR + '/out/2017/04/addon/'
    ])) > 0

    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARIS_TEST_DIR + '/out/2017/04/trunk/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARIS_TEST_DIR + '/payload/2017/04/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARIS_TEST_DIR + '/spark-output/part_provider=caris/'
    ])) > 0


def test_cleanup():

    # cleanup
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARIS_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARIS_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARIS_TEST_DIR + '/spark-output/'
    ])
