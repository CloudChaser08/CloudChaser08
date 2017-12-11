import boto3
import paramiko
import json
import re
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

def lambda_handler(event, context):
    for rec in event['Records']:
        msg = rec['Sns']['Message']
        key = json.loads(msg)['Records'][0]['s3']['object']['key']

    with open('incoming_files_config.json') as infile:
        incoming_files = json.loads(infile.read())

    all_provider_config = {}
    with open('provider_config.json') as infile:
        for provider in json.loads(infile.read()):
            all_provider_config[provider['provider_name']] = provider['configuration']

    matched = False

    for f in incoming_files:
        if re.search(f['s3_key_regex'], key) is not None:
            matched = True
            provider_name = f['provider_name']
            file_date = datetime.strptime(re.search(f['s3_key_regex'], key).group(1), f['date_pattern'])
            break

    if not matched:
        #TODO send to error queue
        return

    s3_client = boto3.client('s3')

    provider_config = all_provider_config[provider_name]
    expected_files = provider_config['expected_files']
    all_here = True

    for expected_file in expected_files:
        s3_key_to_find = datetime.strftime(file_date, expected_file['s3_key_pattern'])
        try:
            s3_client.get_object(Bucket='healthverity', Key=s3_key_to_find)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                all_here = False
                break
            else:
                raise e

    date_offset = provider_config.get('date_offset', {'days': 0})
    if 'days' in date_offset:
        exec_date = file_date + timedelta(days=date_offset['days'])
    else:
        # Hacky way of adding/substracting months
        day = file_date.day
        file_date.replace(day=15)
        exec_date = file_date + timedelta(days=date_offset['months'] * 30)
        exec_date.replace(day=day)

    _trigger_dag(provider_config['dag_name'], datetime.strftime(exec_date, '%Y-%m-%d'))

def _trigger_dag(dag_name, exec_date):
    s3_client = boto3.client('s3')
    s3_client.download_file('healthverityreleases', 'keys/airflow', '/tmp/airflow')
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())

        client.connect('airflow-prod.aws.healthverity.com', port=22, username='airflow', key_filename='/tmp/airflow')

        TRIGGER_COMMAND = "python lambda_dag_trigger.py {} '{}'"
        stdin, stdout, stderr = client.exec_command(TRIGGER_COMMAND.format(dag_name, exec_date))
        print stdout.read()
        print stderr.read()
    finally:
        client.close()

