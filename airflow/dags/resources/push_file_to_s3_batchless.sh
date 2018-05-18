#!/bin/bash
# 12-01-2016
# kyle halpin
# healthverity
# assume role, set creds, push s3 path to s3://hvmatching/inbound/

# Get optargs
while getopts "a:d" opt; do
  case $opt in
    a)
       AUGMENTATION_FILE=$OPTARG;;
    d)
       DEBUG=1;;
    \?)
      echo "Invalid option: -$OPTARG";;
  esac
done

S3_DATA_SOURCE_PATH=${@:$OPTIND:1}
SEQ_NUM=${@:$OPTIND+1:1}
ENVIRONMENT=${@:$OPTIND+2:1}
PRIORITY=${@:$OPTIND+3:1}
WRITE_LOCK=${@:$OPTIND+4:1}
PASSTHROUGH=${@:$OPTIND+5:1}

S3_DESTINATION_PATH="s3://hvmatching/inbound/"

# Generate UUID to create unique messages
UUID=$(cat /proc/sys/kernel/random/uuid)
# If on mac, we won't generate this - so use a default
if [ -z "$UUID" ]; then
  UUID="ABCDEFG";
fi
UUID_SHORT=${UUID:0:4}

# Create a passthrough, augmented, or normal file to process
if [ -n "$PASSTHROUGH" ]; then
  TASK_ID="MORGAN.PASSTHROUGH_${UUID}"
  TASK_ID_SHORT=MORGAN.PASSTHROUGH_${UUID_SHORT}
elif [ -n "$AUGMENTATION_FILE" ]; then
  TASK_ID="MORGAN.AUGMENT_${UUID}"
  TASK_ID_SHORT=MORGAN.AUGMENT_${UUID_SHORT}
else
  TASK_ID="MORGAN.PROCESS_${UUID}"
  TASK_ID_SHORT=MORGAN.PROCESS_${UUID_SHORT}
fi

# Create empty message file and dir if not exisiting
MESSAGE_DIR=/tmp/messages
if [ ! -d "$MESSAGE_DIR" ]; then
mkdir $MESSAGE_DIR
fi
MSG_SOURCE_PATH=$MESSAGE_DIR/$TASK_ID
touch $MSG_SOURCE_PATH

# format destination path
SOURCE_BASE_NAME=$(echo $S3_DATA_SOURCE_PATH | sed -e 's/.*\///')
S3_MSG_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/tasks/$PRIORITY/$(date +%s)_${SEQ_NUM}_${TASK_ID_SHORT}"
S3_DATA_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/data/$SOURCE_BASE_NAME"
S3_DATA_REF_PATH="inbound/$ENVIRONMENT/data/$SOURCE_BASE_NAME"

AUG_BASE_NAME=$(echo "$AUGMENTATION_FILE" | sed -e 's/.*\///')
S3_AUG_REF_PATH="inbound/$ENVIRONMENT/data/$AUG_BASE_NAME"
S3_AUG_DESTINATION_PATH="s3://hvmatching/inbound/$ENVIRONMENT/data/$AUG_BASE_NAME"

# Insert filename as arg into task file
> $MSG_SOURCE_PATH
echo $S3_DATA_REF_PATH >> $MSG_SOURCE_PATH

# If passthrough arg is present, insert flag
if [ -n "$PASSTHROUGH" ]; then
  echo "TRUE" >> $MSG_SOURCE_PATH
# If aug task pass var as arg
elif [ -n "$AUGMENTATION_FILE" ]; then
  echo $S3_AUG_REF_PATH >> $MSG_SOURCE_PATH
fi

echo $WRITE_LOCK >> $MSG_SOURCE_PATH

# setup creds
ROLE_CREDENTIALS=$(/usr/local/bin/aws sts assume-role --role-session-name 'jenkins_push_to_matching' --role-arn 'arn:aws:iam::581191604223:role/hvmatching_writer')
export AWS_SECRET_ACCESS_KEY=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SecretAccessKey')
export AWS_ACCESS_KEY_ID=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.AccessKeyId')
export AWS_SESSION_TOKEN=$(echo $ROLE_CREDENTIALS | jq -r '.Credentials.SessionToken')

# Only execute instructions if we aren't in debug mode
if [ -n "$DEBUG" ]; then
    echo "DEBUG mode, not sending files to s3.  Script Output:"
    echo "DATA SOURCE: $S3_DATA_SOURCE_PATH"
    echo "DATA DEST: $S3_DATA_DESTINATION_PATH"
    echo "PASSTHROUGH?: $PASSTHROUGH"
    echo "AUGMENTATION FILE: $AUGMENTATION_FILE"
    echo "AUG DEST: $S3_AUG_DESTINATION_PATH"

    echo "Reading Message File:"
    cat "$MSG_SOURCE_PATH" | while read line; do
        echo "$line"
    done
else
    echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_DATA_SOURCE_PATH $S3_DATA_DESTINATION_PATH
    echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $MSG_SOURCE_PATH $S3_MSG_DESTINATION_PATH

    # do the copies
    /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $S3_DATA_SOURCE_PATH $S3_DATA_DESTINATION_PATH
    /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control $MSG_SOURCE_PATH $S3_MSG_DESTINATION_PATH

    if [ -n "$AUGMENTATION_FILE" ]; then
      echo /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control "$AUGMENTATION_FILE" "$S3_AUG_DESTINATION_PATH"
      /usr/local/bin/aws s3 cp --sse AES256 --acl bucket-owner-full-control "$AUGMENTATION_FILE" "$S3_AUG_DESTINATION_PATH"
    fi
fi