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


def unzip(tmp_path_template):
    """Unzip correct file in tmp path dir"""
    return lambda ds, **kwargs: check_call([
        'unzip',
        _get_files(tmp_path_template.format(kwargs['ds_nodash']))[0]
    ])


def decrypt(aws_id, aws_key, tmp_path_template):
    """Decrypt correct file in tmp path dir"""
    TMP_DECRYPTOR_LOCATION = "/tmp/decrypt/"
    DECRYPTOR_JAR = TMP_DECRYPTOR_LOCATION + "decryptor.jar"
    DECRYPTOR_KEY = TMP_DECRYPTOR_LOCATION + "key"

    env = os.environ
    env["AWS_ACCESS_KEY_ID"] = aws_id
    env["AWS_SECRET_ACCESS_KEY"] = aws_key

    def execute(ds, **kwargs):
        check_call([
            'aws', 's3', 'cp',
            config.DECRYPTOR_JAR_LOCATION, DECRYPTOR_JAR
        ], env=env)
        check_call([
            'aws', 's3', 'cp',
            config.DECRYPTOR_KEY_LOCATION, DECRYPTOR_KEY
        ], env=env)

        file_list = _get_files(
            tmp_path_template.format(kwargs['ds_nodash'])
        )
        for file_name in file_list:
            check_call([
                'java', '-jar', DECRYPTOR_JAR,
                "-i", file_name, "-o", file_name + ".gz",
                "-k", DECRYPTOR_KEY
            ], env=env)

        check_call([
            'rm', '-r', TMP_DECRYPTOR_LOCATION
        ], env=env)
    return execute


def gunzip(tmp_path_template):
    """Unzip correct gzipped file in tmp path dir"""
    return lambda ds, **kwargs: check_call([
        'gzip', '-d', '-k', '-f',
        _get_files(tmp_path_template.format(kwargs['ds_nodash']))[0]
    ])


def split(tmp_path_template, tmp_path_parts_template, count='20'):
    """Split a file into a specified number of even parts"""
    def execute(ds, **kwargs):
        file_path = filter(
            lambda x: not re.search('.gz$', x),
            _get_files(
                tmp_path_template.format(kwargs['ds_nodash'])
            )
        )[0]
        parts_file_path = tmp_path_parts_template.format(kwargs['ds_nodash'])
        check_call(['mkdir', '-p', parts_file_path])
        check_call(['split', '-n', 'l/' + count, file_path, '{}{}.'
                    .format(parts_file_path, file_path.split('/').last)])
    return execute


def bzip_part_files(tmp_path_parts_template):
    """Bzip each file in a directory"""
    def execute(ds, **kwargs):
        tmp_path_parts = tmp_path_parts_template.format(kwargs['ds_nodash'])
        file_list = _get_files(
            tmp_path_parts_template.format(kwargs['ds_nodash'])
        )
        for file_name in file_list:
            check_call([
                'lbzip2', '{}{}'.format(
                    tmp_path_parts, file_name.split('/').last
                )
            ])
    return execute


def push_splits_to_s3(
        tmp_path_template, tmp_path_parts_template,
        s3_transaction_split_path, aws_id, aws_key
):
    """Push each file in a directory up to a specified s3 location"""
    def execute(ds, **kwargs):
        tmp_path = tmp_path_template.format(kwargs['ds_nodash'])
        tmp_path_parts = tmp_path_parts_template.format(kwargs['ds_nodash'])
        file_name = filter(
            lambda x: x.find('.gz') == (len(x) - 3),
            _get_files(tmp_path)
        )[0].split('/').last
        date = '{}/{}/{}'.format(
            file_name[0:4], file_name[4:6], file_name[6:8]
        )
        env = os.environ
        env["AWS_ACCESS_KEY_ID"] = aws_id
        env["AWS_SECRET_ACCESS_KEY"] = aws_key
        check_call(['aws', 's3', 'cp', '--recursive', tmp_path_parts, "{}{}/"
                    .format(s3_transaction_split_path, date)], env=env)
    return execute
