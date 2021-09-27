import argparse
import os
import re
import subprocess
import psycopg2
import datetime
import boto3
import json
import tempfile


def extract_routine_files(root_dir='.', file_matching_pattern='sparkNormalize*'):
    """ Extract file names for spark routines

    Crawl the dewey package finding all files that match the specified pattern.

    Args:
        root_dir (string): the root directory to begin crawling. this should either be spark/providers or spark/census
        file_matching_pattern (string): the regex to find our routine files

    Returns:
        list: the list of file names

    """
    matches = []
    for root, dirs, files in os.walk(root_dir):
        for file_name in files:
            if re.match(file_matching_pattern, file_name):
                matches.append(os.path.join(root, file_name))
    return matches


def create_mapping(spark_files, census_files, standard_files):
    """ Given a list of file paths for our normal spark_routines plus our census_files,
        create a mapping from routine name to script path and script arguments.

        Returns:
            Map: { ['routine'] : {
                                    ['script_path'] = 'path/to/file.py',
                                    ['script_args'] = 'usage: [h] [date]'
                                 }
                 }

    """
    _mapping = {}
    for script in spark_files + census_files + standard_files:
        parts = script.split('/')

        routine_name = None
        if script in spark_files:
            routine_name = '{}/{}'.format(parts[2], parts[3])
        elif script in census_files:
            routine_name = '{}/{}/{}'.format(parts[1], parts[2], parts[3])

        if routine_name:
            _mapping[routine_name] = {}
            _mapping[routine_name]['script_path'] = script

        if routine_name:
            if routine_name.startswith('census/'):
                _mapping[routine_name]['script_args'] = '--Run me using bin/censusize'
            else:
                module = script.replace('/', '.')[:-3]
                try:
                    usage_output = subprocess.check_output(['python', '-m', module, '--help'])
                    try:
                        usage_output = usage_output.decode()
                    except (UnicodeDecodeError, AttributeError):
                        pass
                    usage_output = re.sub(r'[^a-zA-Z: \.\'\-\[\]]', '', usage_output)
                    usage_output = re.sub(r' +', ' ', usage_output)
                    usage_output = re.sub('optional arguments', '', usage_output)

                except subprocess.CalledProcessError:
                    usage_output = 'NA'

                if len(usage_output) > 1 and usage_output[-1] == '\n':
                    usage_output = usage_output[:-1]
                usage_pattern = 'usage:*'
                if re.match(usage_pattern, usage_output) is None:
                    usage_output = 'NA'
                else:
                    usage_output = usage_output.split(':')[1].strip()

                _mapping[routine_name]['script_args'] = usage_output

    # add censusize
    census_name = 'bin/censusize.py'
    _mapping[census_name] = {}
    _mapping[census_name]['script_path'] = 'spark/bin/censusize.py'
    _mapping[census_name]['script_args'] = '[--batch_id BATCH_ID] [--client_name CLIENT_NAME] ' \
                                           '[--opportunity_id OPPORTUNITY_ID] [--salt SALT] ' \
                                           '[--census_module CENSUS_MODULE] ' \
                                           '[--end_to_end_test] [--test] date'

    return _mapping


def get_reference_db_connection():
    """ Get connection to our reference database. """
    ssm_client = boto3.client('ssm')
    resp = ssm_client.get_parameter(Name='prod-airflow-reference-db_conn', WithDecryption=True)
    creds = resp['Paramater']['Value']
    passwd = json.loads(creds)['password']

    return psycopg2.connect(dbname='request_normalization',
                            user='airflow',
                            password=passwd,
                            host='reference.aws.healthverity.com',
                            port=5432)


def perform_db_updates(_mapping):
    """ Insert mapping entries to DB."""
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    query = "INSERT INTO provider_normalization_routines VALUES (%s, %s, %s, %s)"

    for key, value in _mapping.items():
        with get_reference_db_connection() as conn:
            with conn.cursor() as cur:
                # ~ fresh table
                cur.execute("DELETE FROM provider_normalization_routines")
                cur.execute(query, (key, value['script_path'], current_date, value['script_args']))


def write_to_s3(_mapping):
    """ Write entries to a file daglist.prod.txt and upload to S3.

    The reason for writing to s3 is because I have not figured out how to
    get Jenkins to connect to postgres from the groovy script box which is in an sanboxed environment,
    however, I was able to get jenkins to read content from s3.

    The file is a list of dags in dewey, along with paramaters attached. This data will pre-populate
    the jenkins job with dags as choices in a dropdown box.

    NOTE: although the Jenkins user has access to write to this S3 bucket, you may not on your laptop.

    """
    s3_client = boto3.resource('s3')
    bucket = 'healthverityreleases'
    prefix = 'dewey'
    file_name = 'daglist.prod.txt'
    with tempfile.NamedTemporaryFile() as fp:
        for key, value in sorted(_mapping.items(), key=lambda item: item[0]):
            fp.write('{} {}\n'.format(key, value['script_args']).encode())
        fp.flush()
        s3_client.Bucket(bucket).upload_file(fp.name, '{}/{}'.format(prefix, file_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # TODO: add support for dryrun
    parser.add_argument('--dryrun',
                        help='show what updates would be applied to the reference database, '
                             'but do not actually apply them \n \
                  [WARNING] dryrun is not yet supported. this flag is currently a placeholder',
                        action='store_true',
                        default=True)

    spark_routines = extract_routine_files('spark/providers/', file_matching_pattern='sparkNormalize*')
    census_routines = extract_routine_files('spark/census/', file_matching_pattern='driver*')
    standard_routines = extract_routine_files('spark/delivery/', file_matching_pattern='sparkExtract*')
    mapping = create_mapping(spark_routines, census_routines, standard_routines)
    write_to_s3(mapping)
    perform_db_updates(mapping)
