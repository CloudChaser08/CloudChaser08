from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base  
import db as pipeline_db
import datetime

base = declarative_base()

class FileArrivalLog(base):
    __tablename__ = "file_arrival_log"
    s3_key = Column(String, primary_key=True)

    provider_feed_id = Column(Integer, nullable=False)
    received_dt = Column(DateTime)
    batch_dt = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)

class DataFeedConfiguration(base):
    __tablename__ = "data_feed_configuration"
    id = Column(Integer, primary_key=True)

    hvm_feed_id = Column(Integer, nullable=False)
    feed_name = Column(String, nullable=False)
    dag_name = Column(String, nullable=False)
    grace_period = Column(Integer, nullable=False) # In seconds
    cron_schedule = Column(String, nullable=False)

class DataFeedFileConfiguration(base):
    __tablename__ = "data_feed_file_configuration"
    id = Column(Integer, primary_key=True)

    data_feed_configuration_id = Column(Integer,
            ForeignKey('data_feed_configuration.id'), nullable=False)
    # file name with strftime formatted date
    s3_key_pattern = Column(String, nullable=False)

if __name__ == "__main__":
    db = pipeline_db.get_db_connection()
    base.metadata.create_all(db)
