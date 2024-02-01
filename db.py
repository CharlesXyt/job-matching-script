from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class SQLiteDB:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)

    def get_session(self):
        return sessionmaker(bind=self.engine)() 