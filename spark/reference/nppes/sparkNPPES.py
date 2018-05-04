def pyspark_routine():
    spark = SparkSession.builder.master("local").appName("reference_nppes").getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)

    df = sqlContext.read.csv(file_path, header=True, schema=nppes_schema)

    # need to alias columns, not enough to remove whitespace
    # example "Employer_Identification_Number_(EIN)" -> "ein" -- done

    df.repartition(20).write.parquet()

    df.createOrReplaceTempView("temp_nppes")

    sqlContext.sql("create table ref_nppes as select * from temp_nppes")

    # first drop current ref_nppes table - be sure no data is lost
    sql_new_template = """ ALTER TABLE table_name set location """ + PARQUET_S3_LOCATION
