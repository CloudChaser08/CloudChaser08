import json
from collections import defaultdict

S3_QA_STORE = "s3://salusv/qa_log/{0}/"

def qa(columns, category):
  
  filtered_col = filter(lambda x : (len(x.split("__"))==3 and x.split("__")[0] == category), columns)
  def json_gen(row):
    dic = defaultdict(dict)
    
    for col in filtered_col:
      col_split= col.split("__")
      name,col_type = col_split[0],col_split[1] 
      dic[name].update({col_type : row[col]}) 

    string = json.dumps(dic) 
    return string

  return json_gen

def qa_run(runner, date_col, model, model_version, part_provider, part_date):

    runner.run_spark_script("qa_table.sql")
    qa_df = runner.sqlContext.sql('select * from qa_table')

    table_cols = ["field_name", "field_value", "model", "model_version", "provider", "dataset", "part_date", "source_qa", "output_qa", "errors", "create_date"]
    qa_columns = qa_df.columns
    source_qa = qa(qa_columns, "source")
    output_qa = qa(qa_columns, "output")

    json_df = qa_df.rdd.map(lambda x : (date_col, x[date_col], model, model_version, part_provider, x["data_set"], part_date, source_qa(x), output_qa(x))).toDF(table_cols)

    json_df.write.csv(S3_QA_STORE.format(model))
