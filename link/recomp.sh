for i in $(aws s3 ls s3://salusv/matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/2015 |awk '{print $4}' |sed -e 's/.bz2//')
do
  echo s3://salusv/matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/${i}
  aws s3 cp s3://salusv/matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/${i}.bz2 - | pbzip2 -d -c - | gzip -c - | aws s3 cp - s3://salusv/matching/prodgz/payload/86396771-0345-4d67-83b3-7e22fded9e1d/${i}.gz
done
