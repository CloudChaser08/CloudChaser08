"""multi threaded s3 trasnfer"""
import boto3
from multiprocessing import Pool

THREADS = 64

src_bucket = dest_bucket = src_prefix = dest_prefix = file_list = None


def _copy_file(thread_id):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(dest_bucket)
    source = {
        'Bucket': src_bucket
    }

    for i in range(thread_id, len(file_list), THREADS):
        s3_file = file_list[i]
        source['Key'] = s3_file
        bucket.Object(s3_file.replace(src_prefix, dest_prefix)).copy_from(CopySource=source)


def multithreaded_copy(src, dest):
    src = src + '/' if src[-1] != '/' else src
    dest = dest + '/' if dest[-1] != '/' else dest

    global src_bucket, dest_bucket, src_prefix, dest_prefix, file_list
    src_bucket = src.split('/')[2]
    dest_bucket = dest.split('/')[2]

    src_prefix = src.replace('s3://' + src_bucket + '/', '')
    dest_prefix = dest.replace('s3://' + dest_bucket + '/', '')

    files = []
    s3 = boto3.resource('s3')

    for f in s3.Bucket(src_bucket).objects.filter(Prefix=src_prefix):
        files.append({'key': f.key, 'size': f.size})

    # Sort the keys we want to copy in reverse order by files size
    # This helps even out the workload across different threads
    file_list = [x.get('key') for x in sorted(files, key=lambda x: x.get('size'), reverse=True)]

    p = Pool(THREADS)
    p.map(_copy_file, range(THREADS))
