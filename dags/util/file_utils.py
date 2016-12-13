#
# Utility Functions for processing Files
#
import os
import re
from subprocess import check_call


def _get_files(path):
    file_list = os.listdir(path)
    return map(lambda x: path + x, file_list)


def unzip(tmp_path_template):
    return lambda ds, **kwargs: check_call([
        'unzip',
        _get_files(tmp_path_template.format(kwargs['ds_nodash']))[0]
    ])


def decrypt(ds, **kwargs):
    True
    # todo


def gunzip(tmp_path_template):
    return lambda ds, **kwargs: check_call([
        'gzip', '-d', '-k', '-f',
        _get_files(tmp_path_template.format(kwargs['ds_nodash']))[0]
    ])


def split(tmp_path_template, tmp_path_parts_template):
    def execute(ds, **kwargs):
        file_path = filter(
            lambda x: not re.search('.gz$', x),
            _get_files(
                tmp_path_template.format(kwargs['ds_nodash'])
            )
        )[0]
        parts_file_path = tmp_path_parts_template.format(kwargs['ds_nodash'])
        check_call(['mkdir', '-p', parts_file_path])
        check_call(['split', '-n', 'l/20', file_path, '{}{}.'
                    .format(parts_file_path, file_path.split('/').last)])
    return execute


def bzip_part_files(tmp_path_parts_template):
    def execute(ds, **kwargs):
        tmp_path_parts = tmp_path_parts_template.format(kwargs['ds_nodash'])
        file_list = _get_files(
            tmp_path_parts_template.format(kwargs['ds_nodash'])
        )
        for file_name in file_list:
            check_call([
                'lbzip2', '{}{}'.format(tmp_path_parts, file_name)
            ])
    return execute


def push_splits_to_s3(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])

    file_list = os.listdir(tmp_path)
    file_name = filter(lambda x: x.find('.gz') == (len(x) - 3), file_list)[0]
    date = '{}/{}/{}'.format(file_name[0:4], file_name[4:6], file_name[6:8])
    env = os.environ
    env["AWS_ACCESS_KEY_ID"] = Variable.get('AWS_ACCESS_KEY_ID')
    env["AWS_SECRET_ACCESS_KEY"] = Variable.get('AWS_SECRET_ACCESS_KEY')
    check_call(['aws', 's3', 'cp', '--recursive', tmp_path_parts, "{}{}/"
                .format(S3_TRANSACTION_SPLIT_PATH, date)], env=env)
