import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

db_string = "postgresql://postgres:password@localhost:5432/hkex-ccass"

db = create_engine(db_string)
base = declarative_base()
Session = sessionmaker(bind=db)

database_session = Session()
