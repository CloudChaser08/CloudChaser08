#!/bin/bash
# 12-01-2016
# kyle halpin
# healthverity
# assume role, set creds, push s3 path to s3://hvmatching/inbound/

S3_SOURCE_PATH=$1
SEQ_NUM=$2
ENVIRONMENT=$3
PRIORITY=$4
S3_DESTINATION_PATH="s3://hvmatching/inbound/"

# format destination path

SOURCE_BASE_NAME=$(echo $S3_SOURCE_PATH | sed -e 's/.*\///')
S3_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/tasks/$PRIORITY/$(date +%s)_${SEQ_NUM}_MORGAN.PROCESS_$SOURCE_BASE_NAME"

# setup creds
ROLE_CREDENTIALS=$(/usr/local/bin/aws sts assume-role --role-session-name 'jenkins_push_to_matching' --role-arn 'arn:aws:iam::581191604223:role/hvmatching_writer')

export AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SecretAccessKey')
export AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.AccessKeyId')
export AWS_SESSION_TOKEN=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SessionToken')

echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_SOURCE_PATH $S3_DESTINATION_PATH
# do the copy
/usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_SOURCE_PATH $S3_DESTINATION_PATH
