#! /bin/sh
# Script for manually triggering Normalizations on EMR
# Set the common options, fill in dates in `refactor.dat` and the Census or Marketplace options then follow steps below.
# ~/.ssh/aws_emr is the example path for your SSH key to EMR. You can also make a symlink to that.

# Exit if variables unset or partial command fails.
set -euo pipefail

# (Location) - Description:
#
# IP address from EMR console used as var in later steps.
# (LOCAL) - Setup IP Address:
#   $ export EMR_IP=FILL_IN_IP_ADDRESS_HERE
#
# (LOCAL) - Compile:
#	  $ make package-spark
#
# (LOCAL) - Transfer:
#   $ scp -i ~/.ssh/aws_emr {dewey_spark.tar.gz,refactor.dat,build.sh} hadoop@$EMR_IP:~/
#
# (LOCAL) - Connect EMR :
#	  $ ssh -i ~/.ssh/aws_emr hadoop@$EMR_IP
#
# (EMR shell) - Prerequisites:
#   $ sudo pip-3.4 install boto3 python-dateutil
#   $ tar xvf ./dewey_spark.tar.gz
#
# (EMR shell) - Execute in subprocess:
#   $ nohup sh build.sh 1>>build.out 2>>build.err &
#
# (EMR shell) - Monitor:
#   $ tail -f ./build.out
#
# (EMR shell) - Log out of EMR:
#   $ exit    # or CTRL+D
#
# (LOCAL) - Download report files and logs
#   $ scp -i ~/.ssh/aws_emr hadoop@$EMR_IP:~/{refactor_loaded.dat,refactor_load_failed.dat,build.out,build.err} ./

# Common options
# census OR marketplace
norm_type=""
batch_date=$(date +%F)
date_in_file="refactor.dat"
date_out_file="refactor_loaded.dat"
date_failed_file="refactor_load_failed.dat"

# Marketplace options
script_path="/home/hadoop/spark/providers/vendor/datatype/normalize.py"

# Census options
census_module="spark.census.VENDOR.PATH.driver"
client_name="vendorname"
opp_id="HV000000"

echo "[INFO] [$(TZ=America/New_York date)] Process Started...."
while IFS= read -r line
do
        batch_id="$line"
        # If not blank or commented out
        if [ "${batch_id##\#*}x" != "x" ];
        then
            echo "[INFO] [$(TZ=America/New_York date)] Currently processing $batch_id"
            if [ "${norm_type}" = "census" ];
            then

              /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh /home/hadoop/spark/bin/censusize.py --batch_id ${batch_id} --opportunity_id ${opp_id} --client_name ${client_name} --census_module ${census_module} ${batch_date}"

            elif [ "${norm_type}" = "marketplace" ];
            then

              /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh ${script_path} --date $batch_id"

            fi
            exitCode=$?
            if [ ${exitCode} -eq 0 ];
            then
                echo $batch_id >> "$date_out_file"
                echo "[INFO] [$(TZ=America/New_York date)] Successfully completed $batch_id"
            else
                echo $batch_id >> "$date_failed_file"
                echo "[ERROR] [$(TZ=America/New_York date)] Failed to load $batch_id [exitCode=${exitCode}]"
            fi
        fi
done < "$date_in_file"
echo "[INFO] [$(TZ=America/New_York date)] Process Completed...."
