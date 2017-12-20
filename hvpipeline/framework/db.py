from sqlalchemy import create_engine  
from sqlalchemy.orm import sessionmaker
import os

def get_db_connection():
    db_string = "postgres://{}:{}@{}:{}/{}".format(
        os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'),
        os.getenv('DB_PORT'), os.getenv('DB_SCHEMA')
    )

    return create_engine(db_string)  

def get_db_session():
    db = get_db_connection()
    Session = sessionmaker(bind=db)
    return Session()

def lock_table(session, table_class):
    session.execute('LOCK TABLES :table WRITE',
            {'table': table_class.__tablename__}
    )
