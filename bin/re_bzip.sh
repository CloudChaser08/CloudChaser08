#! /bin/bash

files=$(aws s3 ls s3://salusv/incoming/medicalclaims/emdeon/2015/01/01 --recursive | grep -o 'incoming.*')
for file in $files
do
	fn=$(echo $file | grep -o '[^/]*$' | sed 's/.bz2/./')
	dir=$(echo $file | sed 's/[^/]*$//')
	aws s3 cp s3://salusv/$file - | lbzip2 -d -c > tmp.psv
	split -n l/20 tmp.psv $fn
	lbzip2 $fn*
	aws s3 cp $fn* s3://salusv/$dir
#	Maybe later
#	aws s3 rm s3://salusv/$file --dryrun
done
