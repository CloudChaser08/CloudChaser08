from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base  
import db as pipeline_db
import datetime

base = declarative_base()

class FILE_ARRIVAL_STATUS():
    ARRIVED_ON_TIME = 'FILE ARRIVED ON TIME'
    ARRIVED_LATE    = 'FILE ARRIVED LATE'
    IS_LATE         = 'FILE IS LATE'

class FileArrivalLog(base):
    __tablename__ = "file_arrival_log"
    id = Column(Integer, primary_key=True)

    s3_key = Column(String, nullable=False)
    data_feed_file_configuration_id = Column(Integer,
            ForeignKey('data_feed_file_configuration.id'), nullable=False)
    received_dt = Column(DateTime)
    batch_dt = Column(DateTime, nullable=False)
    status = Column(Enum(FILE_ARRIVAL_STATUS), nullable=False)

class DataFeedConfiguration(base):
    __tablename__ = "data_feed_configuration"
    id = Column(Integer, primary_key=True)

    hvm_feed_id = Column(Integer, nullable=False)
    feed_name = Column(String, nullable=False)
    dag_name = Column(String, nullable=False)
    dag_date_offset = Column(Integer)
    dag_date_offset_qualifier = Column(String)
    grace_period_seconds = Column(Integer, nullable=False)
    cron_schedule = Column(String, nullable=False)
    start_dt = Column(DateTime, nullable=False)

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
