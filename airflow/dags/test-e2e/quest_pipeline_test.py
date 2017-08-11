import subprocess

QUEST_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests'


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/spark-output/'
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'quest_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'quest_pipeline',
        '-s', '2017-03-04T12:00:00',
        '-e', '2017-03-04T12:00:00',
        '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', QUEST_TEST_DIR + '/out/2017/03/01/addon/'
    ])) > 0

    assert len(subprocess.check_output([
        'aws', 's3', 'ls', QUEST_TEST_DIR + '/out/2017/03/01/trunk/'
    ])) > 0


def test_matching_payload_moved():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', QUEST_TEST_DIR + '/payload/2017/03/01/'
    ])) > 0


def test_normalized_data_exists():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', QUEST_TEST_DIR + '/spark-output/part_provider=quest/'
    ])) > 0


def test_cleanup():
    cleanup()
