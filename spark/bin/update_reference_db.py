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


def create_mapping(spark_files, census_files, extract_routines):
    """ Given a list of file paths for our normal spark_routines plus our census_files, 
        create a mapping from routine name to script path and script arguments.

        Returns:
            Map: { ['routine'] : { 
                                    ['script_path'] = 'path/to/file.py',
                                    ['script_args'] = 'usage: [h] [date]'
                                  } 
                 }
    
    """
    mapping = {}
    for script in spark_files + census_files + extract_routines):
        parts = script.split('/')
        
        if script in spark_files:    
            routine_name = '{}/{}'.format(parts[2], parts[3])
        elif script in census_files:
            routine_name = '{}/{}/{}'.format(parts[1], parts[2], parts[3])

        mapping[routine_name] = {}
        mapping[routine_name]['script_path'] = script

        module = script.replace('/', '.')[:-3]
        try :
            usage_output = subprocess.check_output(['python', '-m', module, '--help'])
        except StopIteration:
            usage_output = 'NA'
        
        if len(usage_output) > 1 and usage_output[-1] == '\n':
            usage_output = usage_output[:-1]
        usage_pattern = 'usage:*'
        if re.match(usage_pattern, usage_output) is None:
            usage_output = 'NA'
        else:
            usage_output = usage_output.split(':')[1].strip()
        mapping[routine_name]['script_args'] = usage_output

    return mapping


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


def perform_db_updates(mapping):
    """ Insert mapping entries to DB."""
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    QUERY = "INSERT INTO provider_normalization_routines VALUES (%s, %s, %s, %s)"

    for key, value in mapping.iteritems():
        with get_reference_db_connection() as conn:
            with conn.cursor() as cur:
                # ~ fresh table
                cur.execute("DELETE FROM provider_normalization_routines")
                cur.execute(QUERY, (key, value['script_path'], current_date, value['script_args']))

def write_to_s3(mapping):
    """ Write entries to a file daglist.prod.txt and upload to S3. 

    The reason for writing to s3 is because I have not figured out how to 
    get Jenkins to connect to postgres from the groovy script box which is in an sanboxed environment,
    however, I was able to get jenkins to read content from s3. 

    The file is a list of dags in dewey, along with paramaters attached. This data will pre-populate 
    the jenkins job with dags as choices in a dropdown box. 
    
    NOTE: although the Jenkins user has access to write to this S3 bucket, you may not on your laptop. 
    
    """
    s3_client = boto3.resource('s3')
    BUCKET = 'healthverityreleases'
    PREFIX = 'dewey'
    FILE_NAME = 'daglist.prod.txt'

    with tempfile.TemporaryFile() as fp:
        for key, value in mapping.iteritems():
            fp.write('{} {}\n'.format(key, value['script_args']))
        s3.Bucket(BUCKET).upload_file(fp.name, '{}/{}'.format(PREFIX, FILE_NAME)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
   
    # TODO: add support for dryrun
    parser.add_argument('--dryrun', 
            help='show what updates would be applied to the reference database, but do not actually apply them \n \
                  [WARNING] dryrun is not yet supported. this flag is currently a placeholder',
            action='store_true',
            default=True)
    
    spark_routines = extract_routine_files('spark/providers/', file_matching_pattern='sparkNormalize*')
    census_routines = extract_routine_files('spark/census/', file_matching_pattern='driver*')
    extract_routines = extract_routine_files('spark/delivery/', file_matching_pattern='sparkExtract*')
    mapping = create_mapping(spark_routines, census_routines, delivery_routines)
    write_to_s3(mapping)
    perform_db_updates(mapping)
