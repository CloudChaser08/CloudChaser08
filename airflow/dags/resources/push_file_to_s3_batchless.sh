#!/bin/bash
# 12-01-2016
# kyle halpin
# healthverity
# assume role, set creds, push s3 path to s3://hvmatching/inbound/

S3_DATA_SOURCE_PATH=$1
SEQ_NUM=$2
ENVIRONMENT=$3
PRIORITY=$4
PASSTHROUGH=$5
S3_DESTINATION_PATH="s3://hvmatching/inbound/"

UUID=$(cat /proc/sys/kernel/random/uuid)
UUID_SHORT=${UUID:0:4}

# Create either a process or passthrough task
if [ -n "$PASSTHROUGH" ]; then
  TASK_TYPE="MORGAN.PASSTHROUGH"
else
  TASK_TYPE="MORGAN.PROCESS"
fi
TASK_ID="${TASK_TYPE}_${UUID}"

# Create empty message file and dir if not exisiting
MESSAGE_DIR=/tmp/messages
if [ ! -d "$MESSAGE_DIR" ]; then
mkdir $MESSAGE_DIR
fi
MSG_SOURCE_PATH=$MESSAGE_DIR/$TASK_ID
touch $MSG_SOURCE_PATH

# format destination path
SOURCE_BASE_NAME=$(echo $S3_DATA_SOURCE_PATH | sed -e 's/.*\///')
S3_MSG_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/tasks/$PRIORITY/$(date +%s)_${SEQ_NUM}_${TASK_TYPE}_${UUID_SHORT}"
S3_DATA_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/data/$SOURCE_BASE_NAME"
S3_DATA_REF_PATH="inbound/$ENVIRONMENT/data/$SOURCE_BASE_NAME"

# Insert filename as arg into task file
> $MSG_SOURCE_PATH
echo $S3_DATA_REF_PATH >> $MSG_SOURCE_PATH

# If passthrough args are present, insert them
for a in "${@:6}"
do
    echo $a >> $MSG_SOURCE_PATH
done

# setup creds
ROLE_CREDENTIALS=$(/usr/local/bin/aws sts assume-role --role-session-name 'jenkins_push_to_matching' --role-arn 'arn:aws:iam::581191604223:role/hvmatching_writer')

export AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SecretAccessKey')
export AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.AccessKeyId')
export AWS_SESSION_TOKEN=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SessionToken')

echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_DATA_SOURCE_PATH $S3_DATA_DESTINATION_PATH
echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $MSG_SOURCE_PATH $S3_MSG_DESTINATION_PATH
# do the copies
/usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_DATA_SOURCE_PATH $S3_DATA_DESTINATION_PATH
/usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $MSG_SOURCE_PATH $S3_MSG_DESTINATION_PATH
