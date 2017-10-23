import argparse
import subprocess

CARDINAL_DCOA_TEST_DIR = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/'

def cleanup():
    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_DCOA_TEST_DIR + 'out/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_DCOA_TEST_DIR + 'delivery/'
    ])

    subprocess.check_call([
        'aws', 's3', 'rm', '--recursive', CARDINAL_DCOA_TEST_DIR + 'moved_out/'
    ])


def test_run():
    cleanup()
    subprocss.check_call([
        'airflow', 'clear', '-c', 'cardinal_dcoa_pipeline'
    ])
    subprocess.check_call([
        'airflow', 'backfill', 'cardinal_dcoa_pipeline',
        '-s', '2017-10-09', '-e', '2017-10-09', '-I'
    ])


def test_transactionals_pushed():
    assert len(subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_DCOA_TEST_DIR + 'out/2017/10/10/'
    ])) > 0


def test_delivery_file_created():
    assert '_SUCCESS' in subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_DCOA_TEST_DIR + 'delivery/2017/10/10/'
    ])


def test_delivery_file_pushed():
    assert len(filter(lambda x: x != '', subprocess.check_output([
        'aws', 's3', 'ls', CARDINAL_DCOA_TEST_DIR + 'moved_out/'
    ]).split('\n'))) == 1


def run_tests():
    test_transactionals_pushed()
    test_delivery_file_created()
    test_delivery_file_pushed()


def main(args):
    if not args.tests_only:
        test_run()
    run_tests()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tests_only', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

