import boto3
import paramiko

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    s3_client.download_file('healthverityreleases', 'keys/airflow', '/tmp/airflow')
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy)

        client.connect('airflow-prod.aws.healthverity.com', port=22, username='airflow', key_filename='/tmp/airflow')

        stdin, stdout, stderr = client.exec_command('touch test')
        print stdout.read()
        print stderr.read()
    finally:
        client.close()

