import boto3

def get_s3_data(path_str, show_partitions=False):
    bucket_name = path_str.split('/')[2]
    main_file_path = '/'.join(path_str.split('/')[3:])
    results_dict = {'file_paths': [], 'file_name': [], 'add_file_time': [], 'file_size': [], 'file_date': [],
                    'add_partition': []}

    buk_obj = boto3.resource('s3').Bucket(bucket_name)

    for obj in buk_obj.objects.filter(Prefix=main_file_path):
        obj_key = buk_obj.Object(obj.key)

        results_dict['file_paths'].append(
            's3://' + bucket_name + '/' + str(obj_key).split("'", 3)[-1].rsplit('/', 1)[0] + '/')
        results_dict['file_name'].append(str(obj_key).split('/', 7)[-1].split(')', 1)[0].split("'", 1)[0])
        results_dict['add_file_time'].append(obj_key.last_modified)
        results_dict['file_size'].append(obj_key.content_length)
        results_dict['file_date'].append(str(obj_key).split('/', 3)[-1][0:10])
        results_dict['add_partition'].append(
            'PARTITION (part_processdate = "' + results_dict['file_date'][-1] + '") LOCATION "' +
            results_dict['file_paths'][-1] + '"')

    df = spark.createDataFrame(zip(results_dict['file_paths'], results_dict['file_name'], results_dict['add_file_time'],
                                   results_dict['file_size'], results_dict['file_date'], results_dict['add_partition']),
                               schema=['file_path', 'file_name', 'add_file_time', 'file_size', 'file_date',
                                       'add_partition'])
    df_summary = df.select('file_path', 'file_name', 'add_file_time', 'file_size', 'file_date').createOrReplaceTempView(
        "summary")
    df_partition = df.select('add_partition').dropDuplicates().orderBy('add_partition', ascending=True)
    df_partition.createOrReplaceTempView("partitions")

    print('main results are in temporary table: <summary>')
    print('add partitions are in temporary table: <partitions>')
    print('\n')

    if show_partitions == True:
        for row in df_partition.collect():
            print(row[0])