#
# Utility Functions for processing Files
#
import os
import re
from subprocess import check_call
import config


def _get_files(path):
    """Get all files in a path"""
    file_list = os.listdir(path)
    return map(lambda x: path + x, file_list)


def get_s3_env(aws_id, aws_key):
    env = os.environ
    env["AWS_ACCESS_KEY_ID"] = aws_id
    env["AWS_SECRET_ACCESS_KEY"] = aws_key
    return env


def fetch_file_from_s3(aws_id, aws_key, s3_path, local_path):
    """Download a file from S3"""
    env = get_s3_env(aws_id, aws_key)
    check_call(['aws', 's3', 'cp', s3_path, local_path], env=env)


def unzip(path):
    """Unzip correct file in tmp path dir"""
    zip_file = filter(
        lambda x: x.endswith(".zip"),
        _get_files(path)
    )[0]
    check_call([
        'unzip', zip_file, '-d', path
    ])
    check_call([
        'rm', zip_file
    ])


def decrypt(aws_id, aws_key, decryptor_path, file_path):
    """Decrypt correct file in tmp path dir"""
    DECRYPTOR_JAR = decryptor_path + "decryptor.jar"
    DECRYPTOR_KEY = decryptor_path + "private.reformat"

    env = get_s3_env(aws_id, aws_key)

    check_call(["mkdir", decryptor_path], env=env)
    check_call([
        "aws", "s3", "cp", config.DECRYPTOR_JAR_LOCATION, DECRYPTOR_JAR
    ], env=env)
    check_call([
        "aws", "s3", "cp", config.DECRYPTOR_KEY_LOCATION, DECRYPTOR_KEY
    ], env=env)

    file_name = _get_files(file_path)[0]
    check_call([
        "java", "-jar", DECRYPTOR_JAR,
        "-i", file_name, "-o", file_name + ".gz",
        "-k", DECRYPTOR_KEY
    ], env=env)

    check_call(["rm", "-rf", decryptor_path], env=env)


def gunzip(path):
    """Unzip correct gzipped file in tmp path dir"""
    check_call([
        'gzip', '-d', '-k', '-f',
        filter(
            lambda x: x.endswith('.gz'),
            _get_files(path)
        )[0]
    ])


def split(path, parts_path, count='20'):
    """Split a file into a specified number of even parts"""
    file_path = filter(
        lambda x: not re.search('.gz$', x),
        _get_files(path)
    )[0]
    check_call(['mkdir', '-p', parts_path])
    check_call(['split', '-n', 'l/' + count, file_path, '{}{}.'
                .format(parts_path, file_path.split('/')[-1])])


def bzip_part_files(parts_path):
    """Bzip each file in a directory"""
    file_list = _get_files(
        parts_path
    )
    for file_name in file_list:
        check_call([
            'lbzip2', '{}{}'.format(
                parts_path, file_name.split('/')[-1]
            )
        ])


def push_splits_to_s3(
        parts_path, s3_transaction_split_path, aws_id, aws_key
):
    """Push each file in a directory up to a specified s3 location"""
    env = get_s3_env(aws_id, aws_key)
    check_call(['aws', 's3', 'cp', '--recursive', parts_path, "{}"
                .format(s3_transaction_split_path)], env=env)
