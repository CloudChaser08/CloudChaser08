
import logging
from airflow.hooks.hive_hooks import HiveServer2Hook

def hive_execute(sqls, conn_id='hive_analytics'):
    hive_hook = HiveServer2Hook(hiveserver2_conn_id=conn_id)
    conn = hive_hook.get_conn()
    with conn.cursor() as cur:
        logging.info("Setting SSE")
        cur.execute('set fs.s3a.server-side-encryption-algorithm=AES256')
        cur.execute('set fs.s3n.server-side-encryption-algorithm=AES256')
        cur.execute('set hive.strict.checks.cartesian.product=false')
        cur.execute('set hive.mapred.mode=nonstrict')
        for statement in sqls:
            logging.info("SQL: " + statement)
            cur.execute(statement)


