import subprocess
from datetime import datetime


MCKESSON_TEST_DIR ='s3://healthverityconsent/outgoing/testing/'

def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', MCKESSON_TEST_DIR
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'consent_mckesson_extract'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'consent_mckesson_extract',
        '-s', datetime.utcnow(),
        '-e', datetime.utcnow(),
        '-I'
    ])


def test_export_created():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', MCKESSON_TEST_DIR
    ])) > 0

def test_cleanup():
    cleanup()
