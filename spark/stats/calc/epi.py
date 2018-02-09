import boto3

def calculate_epi(provider_conf, field):
    feed_id = provider_conf['feed_id']

    s3_bucket = 'healthverityreleases'
    s3_key_template = 'PatientIntersector/hll_seq_data_store/'
                      + 'patient/{}/{}/manifest/part-0000'
    s3_client = boto3.client('s3')

    respone = s3_client.get_object(
                        Bucket = s3_bucket,
                        Key = s3_key_template.format(feed_id, field)
             )
    file_stream = response['Body']

    output = []
    for line in file_stream:
        res = line.split(';')[::-2]
        output.append({'field': res[1], 'value': res[0]})

    return output


