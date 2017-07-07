#!/bin/bash
# khalpin
# 7/28/2016
# run gsdd update and export

aws s3 cp s3://salusv/reference/gsdd/GSDD.db /opt/goldstandard/gsdd5/
if [ $? -ne 0 ]; then
	echo "Problem downloading GSDD.db"
	exit 1
fi

aws s3 cp s3://salusv/reference/gsdd/GSDDMonograph.db /opt/goldstandard/gsdd5/
if [ $? -ne 0 ]; then
	echo "Problem downloading GSDDMonograph.db"
	exit 1
fi

# all else bail automatically 
set -e
GSDDUpdate
GSDDServerStart
GSDDExport


for x in $(ls /opt/goldstandard/gsdd5/Export/Table/); do
	# get short name
	base_name=$(basename $x);

	# lop off the file extension
	dir_name=${base_name::-4};

	# copy with dir name
	aws s3 cp /opt/goldstandard/gsdd5/Export/Table/$x s3://salusv/reference/gsdd/${dir_name}/$x;
done


# now, we source the env var info, and run this into redshift
git clone git@github.com:healthverity/dewey.git

. /root/.reference_data_env

cd dewey/providers/gsdd/

#make drop
#make import
