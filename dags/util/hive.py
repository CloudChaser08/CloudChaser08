
from airflow.hooks.hive_hooks import HiveServer2Hook

def hive_execute(sqls, conn_id='hive_analytics'):
    hive_hook = HiveServer2Hook(hiveserver2_conn_id=conn_id)
    conn = hive_hook.get_conn()
    with conn.cursor() as cur:
        print("Setting SSE\n")
        cur.execute('set fs.s3a.server-side-encryption-algorithm=AES256')
        for statement in sqls:
            print("SQL: " + statement + "\n")
            cur.execute(statement)


