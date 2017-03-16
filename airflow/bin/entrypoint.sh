#!/usr/bin/env bash

ENTRY_OPTION=$1

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"

# Generate Fernet key
: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")} sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

sed -i "s|\$WEBSERVER_HOST|$DOCKER_IP|" "$AIRFLOW_HOME"/airflow.cfg

echo "Reset database..."

touch $AIRFLOW_HOME/airflow.db

$CMD initdb
$CMD resetdb -y

sqlite3 $AIRFLOW_HOME/airflow.db \
  "INSERT INTO connection (conn_id, conn_type, host, schema, login, password, port, extra, is_encrypted, is_extra_encrypted)
    VALUES ('hive_analytics','hiveserver2','analytics.aws.healthverity.com','default','hadoop','','10000','',0,0)"

sqlite3 $AIRFLOW_HOME/airflow.db \
  "INSERT INTO variable (\"key\", val, is_encrypted)
   VALUES ('DECRYPTOR_JAR_REMOTE_LOCATION','gAAAAABYi84qao5VTHGJ6-u-7xmtPnDJdoNxtpF5Y5Qrr3EPWP0nBLaKWJsfw61XRzKy558zYXfJ_m0VdNpCEZqfdkGijBrKHm0c5lr0epE8oUBn07DkN03OxZ9DgLyW3demkb6R0Tu1gV0ze_tSTIX_J8HcMrCtrQ==',1)"

sqlite3 $AIRFLOW_HOME/airflow.db \
  "INSERT INTO variable (\"key\", val, is_encrypted)
   VALUES ('DECRYPTION_KEY_REMOTE_LOCATION','gAAAAABYi8416gMCIkCxvtPDz7GrXJWOoPKvWGFu42B6LJc0kHAcrMv_o0ZhsHDIcCPFXujI06zCQbg4epduYKJKVU6LW2lkFVwCUBiuf0n1EYmCC7ydLC7xSqPK9Tc_A13hFpB2p4H-qHdh2-VnAkJ6SIN87C11RQ==',1)"

sqlite3 $AIRFLOW_HOME/airflow.db \
  "INSERT INTO variable (\"key\", val, is_encrypted)
   VALUES ('SlackToken','gAAAAABYjE_8QzxBp-NiGrm1MpI-ldKBju_A8CaIZmtbZb8w3zGUPrhtlwL456PTsZtBQlhltWS3P404A8EyaBwLlHPJAqfwHA==',1)"

sqlite3 $AIRFLOW_HOME/airflow.db \
  "INSERT INTO variable (\"key\", val, is_encrypted)
   VALUES ('DATADOG_KEYS','gAAAAABYpzpc1doNml_ZlHt0d5acGAWc5bFPQ65NtBGZS101zKsEiD_qzoevLQfMoFxjkRfZZZUVroeFhnIZGgIXhBmougGHEsSwRj5wYRsOL3pQdnCbgUYMfmGuOTvhjsKAVhXFG_4u67hcpdKXj_WSl8gzH6emvyCXj3WLlRfLdxWGUeODeGKQCSGYPN4ka38l7KKd0DSrfGXMkvMKAaZ_XZYPC7AvyWGlaRlULvNlR5gYNzAxDLc=',1)"

$CMD initdb

case $ENTRY_OPTION in

  runtests)
    cd $AIRFLOW_HOME/dags
    exec nosetests -w test/ -v
    ;;

  *)
    echo "Starting Webserver..."
    $CMD webserver -D # --stderr /dev/stdout

    echo "Starting Scheduler..."
    exec $CMD scheduler
    ;;

esac
