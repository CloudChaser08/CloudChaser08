import boto3

def _get_s3_file_contents(bucket, key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
                        Bucket = bucket,
                        Key = key
             )
    file_data = response['Body'].read()
    return file_data


def calculate_epi(provider_conf, field):
    feed_id = provider_conf['datafeed_id']

    s3_bucket = 'healthverityreleases'
    s3_key = 'PatientIntersector/hll_seq_data_store/' \
              + 'patient/{}/{}/manifest/part-00000' \
              .format(feed_id, field)

    file_data = _get_s3_file_contents(s3_bucket, s3_key)

    output = []
    for line in file_data.split('\n'):
        if len(line.strip()) != 0:
            res = line.split(';')[2:4]
            output.append({'field': res[1], 'value': res[0]})

    return output


