import subprocess

QUEST_TEST_DIR = 's3://healthveritydev/musifer/tests/airflow/quest'


def test_run():
    subprocess.check_call([
        'airflow', 'clear', '-c', 'quest_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'quest_pipeline',
        '-s', '2017-03-02T12:00:00',
        '-e', '2017-03-02T12:00:00',
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


def test_cleanup():

    # cleanup
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/payload/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', QUEST_TEST_DIR + '/spark-output/'
    ])
