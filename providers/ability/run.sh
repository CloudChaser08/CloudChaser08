#!/bin/bash

CLUSTER_ID=$1

export PGHOST="${CLUSTER_ID}.cz8slgfda3sg.us-east-1.redshift.amazonaws.com"
export PGPORT='5439'
export PGDATABASE='dev'
export PGUSER='hvuser'
export PGPASSWORD='HV1user2'

S3_CREDS="aws_access_key_id=$AWS_ACCESS_KEY_ID;aws_secret_access_key=$AWS_SECRET_ACCESS_KEY"
BASE_PATH="s3://salusv/incoming/medicalclaims/ability/"
MATCHING_PATH="s3://salusv/matching/payload/medicalclaims/ability/"
OUTPUT_PATH="s3://salusv/warehouse/text/medicalclaims/ability/"
NEW_CLAIMAFFILIATION="s3://salusv/incoming/medicalclaims/ability/vwclaimaffiliation_correction_20170215/ap_vwclaimaffiliation.txt.20140101_20170215.bz2"

# HISTORICAL LOAD for AP
load_claimaff="true"
for f in $(aws s3 ls --recursive $BASE_PATH | awk '{print $4}' | cut -d_ -f1 | sort -u | head -16 | grep 'ap$')
do
  app=$(echo $f | cut -d_ -f1 | rev | cut -d/ -f1 | rev)
  full_path="s3://salusv/$(echo $f | rev | cut -d/ -f2- | rev)"
  setid="$(echo $full_path | cut -d/ -f7- | sed 's/\//_/g')_$app"

  if [ "$setid" != "2014_12_ap" ]
  then
    echo "Processing $setid"

    if [ $load_claimaff = "true" ]
    then
      load_claimaff="false"
      python rsNormalizeAbility.py                                                           \
             --header_path ${full_path}/${app}_record.vwheader.txt                           \
             --serviceline_path ${full_path}/${app}_vwserviceline.txt                        \
             --servicelineaffiliation_path ${full_path}/${app}_vwservicelineaffiliation.txt  \
             --claimaffiliation_path $NEW_CLAIMAFFILIATION                                   \
             --diagnosis_path ${full_path}/${app}_vwdiagnosis.txt                            \
             --procedure_path ${full_path}/${app}_vwprocedurecode.txt                        \
             --billing_path ${full_path}/${app}_vwbilling.txt                                \
             --payer_path ${full_path}/${app}_record.vwpayer.txt                             \
             --matching_path ${MATCHING_PATH}${setid}                                        \
             --output_path $OUTPUT_PATH                                                      \
             --database $PGDATABASE                                                          \
             --setid $setid                                                                  \
             --cluster_endpoint $PGHOST                                                      \
             --s3_credentials $S3_CREDS                                                      \
             --rs_user $PGUSER                                                               \
             --rs_password $PGPASSWORD                                                       \
             --load_claimaffiliation
    else
      python rsNormalizeAbility.py                                                           \
             --header_path ${full_path}/${app}_record.vwheader.txt                           \
             --serviceline_path ${full_path}/${app}_vwserviceline.txt                        \
             --servicelineaffiliation_path ${full_path}/${app}_vwservicelineaffiliation.txt  \
             --claimaffiliation_path $NEW_CLAIMAFFILIATION                                   \
             --diagnosis_path ${full_path}/${app}_vwdiagnosis.txt                            \
             --procedure_path ${full_path}/${app}_vwprocedurecode.txt                        \
             --billing_path ${full_path}/${app}_vwbilling.txt                                \
             --payer_path ${full_path}/${app}_record.vwpayer.txt                             \
             --matching_path ${MATCHING_PATH}${setid}                                        \
             --output_path $OUTPUT_PATH                                                      \
             --database $PGDATABASE                                                          \
             --setid $setid                                                                  \
             --cluster_endpoint $PGHOST                                                      \
             --s3_credentials $S3_CREDS                                                      \
             --rs_user $PGUSER                                                               \
             --rs_password $PGPASSWORD
    fi
  fi

done

# # HISTORICAL LOAD for other apps
# for f in $(aws s3 ls --recursive $BASE_PATH | awk '{print $4}' | cut -d_ -f1 | sort -u | head -16 | grep -v 'ap$')
# do
#   app=$(echo $f | cut -d_ -f1 | rev | cut -d/ -f1 | rev)
#   full_path="s3://salusv/$(echo $f | rev | cut -d/ -f2- | rev)"
#   setid="$(echo $full_path | cut -d/ -f7- | sed 's/\//_/g')_$app"

#   echo "Processing $setid"

#   python rsNormalizeAbility.py                                                           \
#          --header_path ${full_path}/${app}_record.vwheader.txt                           \
#          --serviceline_path ${full_path}/${app}_vwserviceline.txt                        \
#          --servicelineaffiliation_path ${full_path}/${app}_vwservicelineaffiliation.txt  \
#          --claimaffiliation_path ${full_path}/${app}_vwclaimaffiliation.txt              \
#          --diagnosis_path ${full_path}/${app}_vwdiagnosis.txt                            \
#          --procedure_path ${full_path}/${app}_vwprocedurecode.txt                        \
#          --billing_path ${full_path}/${app}_vwbilling.txt                                \
#          --payer_path ${full_path}/${app}_record.vwpayer.txt                             \
#          --matching_path ${MATCHING_PATH}${setid}                                        \
#          --output_path $OUTPUT_PATH                                                      \
#          --database $PGDATABASE                                                          \
#          --setid $setid                                                                  \
#          --cluster_endpoint $PGHOST                                                      \
#          --s3_credentials $S3_CREDS                                                      \
#          --rs_user $PGUSER                                                               \
#          --rs_password $PGPASSWORD                                                       \
#          --load_claimaffiliation
# done

# DAILY LOAD
for i in $(seq 0 65)
do
  d=$(date -d "2016-12-12 + $i day" '+%Y-%m-%d')
  
  echo "Processing daily $d"

  python rsNormalizeAbilityDaily.py                    \
         --date $d                                     \
         --s3_credentials $S3_CREDS

done

