import boto3

from ..models.results import GenericStatsResult, GenericStatsResultSet

def _get_s3_file_contents(bucket, key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
                        Bucket = bucket,
                        Key = key
             )
    file_data = response['Body'].read()
    return file_data


def calculate_epi(provider_conf, field):
    feed_id = provider_conf.datafeed_id

    s3_bucket = 'healthverityreleases'
    s3_key = 'PatientIntersector/hll_seq_data_store/' \
              + 'patient/{}/{}/manifest/part-00000' \
              .format(feed_id, field)

    file_data = _get_s3_file_contents(s3_bucket, s3_key)

    output = []
    for line in file_data.split('\n'):
        if len(line.strip()) != 0:
            res = line.strip().split(';')[2:]
            value = res[0]
            output_field = ';'.join(res[1:])
            output.append(
                GenericStatsResult(
                    field=output_field,
                    value=int(value)
                )
            )

    return GenericStatsResultSet(results=output)
