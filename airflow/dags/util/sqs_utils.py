import boto3

def send_messages(queue_url, messages):
    for m in messages:
        send_message(queue_url, m)

def send_message(queue_url, message):
    sqs = boto3.client('sqs')
    sqs.send_message(QueueUrl=queue_url, MessageBody=message)

def is_empty(queue_url):
    sqs = boto3.client('sqs')
    return 'Messages' not in sqs.receive_message(QueueUrl=queue_url, VisibilityTimeout=0)

def delete_messages(queue_url, receipt_handles):
    for rh in receipt_handles:
        delete_message(queue_url, rh)

def delete_message(queue_url, receipt_handle):
    sqs = boto3.client('sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

def get_messages(queue_url, count=None, visibility_timeout=30):
    sqs = boto3.client('sqs')
    count = count or 10000000 # Some really big number
    msgs = []
    while count > 0:
        cnt = 10 if count > 10 else count
        resp = sqs.receive_message(queue_url, MaxNumberOfMessages=cnt, VisibilityTimeout=visibility_timeout)

        if 'Messages' not in resp:
            break
        msgs += resp['Messages']

    return msgs
