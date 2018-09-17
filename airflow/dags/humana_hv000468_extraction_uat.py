import humana_hv000468_extraction
for m in [humana_hv000468_extraction]:
    reload(m)

DAG_NAME='humana_hv000468_extraction_uat'
EMR_CLUSTER_NAME='humana-data-extraction-uat'
START_DATE=datetime(2018, 9, 18)
HUMANA_INBOX='https://sqs.us-east-1.amazonaws.com/581191604223/humana-inbox-uat'

humana_hv000468_extraction.create_humana_dag(DAG_NAME, EMR_CLUSTER_NAME, HUMANA_INBOX, START_DATE, False)
