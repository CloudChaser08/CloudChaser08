import boto3
import paramiko
import json
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import hvpipeline.models as pipeline_models
import hvpipeline.db as pipeline_db

def lambda_handler(event, context):
    for rec in event['Records']:
        msg = rec['Sns']['Message']
        key = json.loads(msg)['Records'][0]['s3']['object']['key']

    session = pipeline_db.get_db_session()
    configured_files = session.query(pipeline_models.DataFeedFileConfiguration).all()

    matching_configurations = []
    for f in configured_files:
        try:
            file_date = datetime.strptime(key, f.s3_key_pattern)
            matching_configurations.append(f)
        except ValueError:
            continue

    if not matching_configurations:
        #TODO send to error queue
        return

    feeds_all_files_arrived = []

    session.begin()
    # Lock table to prevent a race condition
    pipeline_db.lock(session, pipeline_models.FileArrivalLog)
    try:
        for conf in matching_configurations:
            new_file = pipeline_models.FileArrivalLog(
                    s3_key=key,
                    data_feed_file_configuration_id=conf.id
                    received_dt=datetime.utcnow(),
                    batch_dt=file_date,
                    status=pipeline_models.FILE_ARRIVAL_STATUS.ARRIVED_ON_TIME
            )

            session.add(new_file)

            ingested_files = session.query(pipeline_models.FileArrivalLog).\
                    join(pipeline_models.DataFeedFileConfiguration).\
                    select(pipeline_models.FileArrivalLog.s3_key).\
                    filter_by(batch_dt=file_date, data_feed_configuration_id=conf.feed_config_id).all()

            keys_logged = set([f.s3_key for f in ingested_files] + [key])

            expected_file_configurations = session.query(
                            pipeline_models.DataFeedFileConfiguration
                        ).\
                    select(pipeline_models.DataFeedFileConfiguration.s3_key_pattern).\
                    filter_by(data_feed_configuration_id=conf.feed_config_id).all()

            expected_keys = [file_date.strftime(f.s3_key_pattern) for f in
                    expected_file_configurations]

            if not expected_keys.difference(keys_logged):
                feeds_all_files_arrived.append(conf.feed_config_id)

        session.commit()
    except:
        #TODO send to error queue
        session.rollback()

    for feed_config_id in feeds_all_files_arrived:
        feed_config = session.query(pipeline_models.DataFeedConfiguration).\
                filter_by(id=feed_config_id)

        date_offset_qualifier = 'days'
        date_offset = 0
        if feed_config.dag_date_offset_qualifier:
            date_offset_qualifier = feed_config.dag_date_offset_qualifier
            date_offset = feed_config.dag_date_offset

        if date_offset_qualifier in ('days', 'hours'):
            exec_date = file_date + timedelta(**{date_offset_qualifier : date_offset})
        else:
            # Hacky way of adding/substracting months
            day = file_date.day
            file_date.replace(day=15)
            exec_date = file_date + timedelta(days=date_offset * 30)
            exec_date.replace(day=day)

        _trigger_dag(provider_config.dag_name, datetime.strftime(exec_date, '%Y-%m-%dT%H%M%S'))

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

