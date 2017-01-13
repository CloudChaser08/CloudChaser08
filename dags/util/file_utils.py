#
# Utility Functions for processing files
#
import os
import re
import aws_utils
from subprocess import check_call


def _get_files(path):
    """Get all files in a path"""
    file_list = os.listdir(path)
    return map(lambda x: path + x, file_list)


def unzip(path):
    """Unzip correct file in tmp path dir"""
    zip_file = filter(
        lambda x: x.endswith(".zip"),
        _get_files(path)
    )[0]
    check_call([
        'unzip', '-o', zip_file, '-d', path
    ])
    check_call([
        'rm', zip_file
    ])


def decrypt(aws_id, aws_key, decryptor_path, file_path):
    """Decrypt correct file in tmp path dir"""
    import config
    DECRYPTOR_JAR = decryptor_path + "decryptor.jar"
    DECRYPTOR_KEY = decryptor_path + "private.reformat"

    env = aws_utils.get_aws_env()

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
