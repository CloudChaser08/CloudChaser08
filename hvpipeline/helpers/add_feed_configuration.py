import hvpipeline.framework.db as pipeline_db
import hvpipeline.framework.models as pipeline_models
import sys
from datetime import datetime

session = pipeline_db.get_db_session()
feed_config = pipeline_models.DataFeedConfiguration(
        hvm_feed_id=sys.argv[1],
        feed_name=sys.argv[2],
        dag_name=sys.argv[3],
        dag_date_offset=int(sys.argv[4]),
        dag_date_offset_qualifier=sys.argv[5],
        grace_period=int(sys.argv[6]),
        cron_schedule=sys.argv[7],
        start_dt=datetime.strptime('%Y-%m-%dT%H%M%S')
)
session.add(feed_config)
session.commit()
