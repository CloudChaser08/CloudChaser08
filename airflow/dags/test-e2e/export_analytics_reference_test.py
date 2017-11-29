import subprocess

REFERENCE_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/export_analytics_reference/')


def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', REFERENCE_TEST_DIR
    ])


def test_run():
    cleanup()
    subprocess.check_call([
        'airflow', 'clear', '-c', 'export_analytics_reference_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'export_analytics_reference_pipeline',
        '-s', '2017-11-16T00:00:00', '-e', '2017-11-16T00:00:00'
    ])


def test_tables_exported():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', REFERENCE_TEST_DIR
    ])) > 0

def test_cleanup():
    cleanup()
