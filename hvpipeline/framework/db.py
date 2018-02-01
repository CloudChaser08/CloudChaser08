from sqlalchemy import create_engine  
from sqlalchemy.orm import sessionmaker
import os

def get_db_connection():
    db_string = "postgres://{}:{}@{}:{}/{}".format(
        os.getenv('PGUSER'), os.getenv('PGPASSWORD'), os.getenv('PGHOST'),
        os.getenv('PGPORT'), os.getenv('PGDATABASE')
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
