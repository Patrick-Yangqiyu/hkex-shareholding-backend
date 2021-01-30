# coding: utf-8
import json

from sqlalchemy import BigInteger, Column, Date, Float, Integer, Text, text
from sqlalchemy.ext.declarative import declarative_base
from database import database_session

Base = declarative_base()
metadata = Base.metadata
from sqlalchemy.sql import exists, and_
from sqlalchemy.sql.expression import true


class Detail(Base):
    __tablename__ = 'detail'
    __table_args__ = {'schema': 'hkex'}

    Id = Column(BigInteger, primary_key=True)
    ParticipantCode = Column(Text, nullable=False)
    StockCode = Column(Text, nullable=False)
    Shareholding = Column(BigInteger)
    Percentage = Column(Float(53))
    RecordDate = Column(Date)
    ParticipantName = Column(Text)
    Address = Column(Text)

    @classmethod
    def exist(cls, code, date, pcode, pname):
        """
        :param code: stock code
        :param date: datetime date
        :return: bool
        """
        return database_session.query(exists().where(and_(cls.StockCode == code,
                                                          cls.RecordDate == date, cls.ParticipantCode == pcode,
                                                          cls.ParticipantName == pname))).scalar()

    @classmethod
    def update(cls, code, date, pcode, pname, **kwargs):
        database_session.query(cls).filter(
            and_(cls.StockCode == code, cls.RecordDate == date, cls.ParticipantCode == pcode,
                 cls.ParticipantName == pname)).update(kwargs)
        database_session.commit()

    @property
    def toJSON(self):
        return {

            'Id': self.Id,
            'ParticipantCode': self.ParticipantCode.strip(),
            'StockCode': self.StockCode,
            'Shareholding': self.Shareholding,
            'Percentage': self.Percentage,
            'RecordDate': self.RecordDate.strftime('%Y-%m-%d'),
            'ParticipantName': self.ParticipantName.strip(),
            'Address': self.Address,

        }


class Transaction(Detail):
    DiffShs = Column(BigInteger)
    DiffPercentage = Column(Float(53))

    @property
    def toJSON(self):
        return {

            'Id': self.Id,
            'ParticipantCode': self.ParticipantCode.strip(),
            'StockCode': self.StockCode,
            'Shareholding': self.Shareholding,
            'Percentage': self.Percentage,
            'RecordDate': self.RecordDate.strftime('%Y-%m-%d'),
            'ParticipantName': self.ParticipantName.strip(),
            'Address': self.Address,
            'DiffShs': self.DiffShs,
            'DiffPercentage': self.Percentage,
        }


class Participant(Base):
    __tablename__ = 'participant'
    __table_args__ = {'schema': 'hkex'}

    Id = Column(Integer, primary_key=True, server_default=text("nextval('hkex.\"participant_Id_seq\"'::regclass)"))
    ParticipantCode = Column(Text)
    ParticipantName = Column(Text)
    ParticipantNameCht = Column(Text)
    UpdateDate = Column(Date)

    @classmethod
    def exist(cls, code, name, date):
        """
        :param code: Participant code
        :param date: datetime date
        :param date: Participant name

        :return: bool
        """
        return database_session.query(exists().where(and_(cls.ParticipantCode == code, cls.ParticipantName == name,
                                                          cls.UpdateDate == date))).scalar()


class Snapshot(Base):
    __tablename__ = 'snapshot'
    __table_args__ = {'schema': 'hkex'}

    Id = Column(BigInteger, primary_key=True, server_default=text("nextval('hkex.\"snapshot_Id_seq\"'::regclass)"))
    StockCode = Column(Text)
    MarketIntermediariesShareholding = Column(BigInteger)
    MarketIntermediariesParticipantNum = Column(Integer)
    MarketIntermediariesPercentage = Column(Float(53))
    ConsentingInvestorShareholding = Column(BigInteger)
    ConsentingInvestorNum = Column(Integer)
    ConsentingInvestorPercentage = Column(Float(53))
    NonConsentingInvestorShareholding = Column(BigInteger)
    NonConsentingInvestorNum = Column(Integer)
    NonConsentingInvestorPercentage = Column(Float(53))
    TotalShareholding = Column(BigInteger)
    RecordDate = Column(Date)
    TotalNum = Column(Integer)
    TotalPercentage = Column(Float(53))
    TotalIssued = Column(BigInteger)

    @classmethod
    def exist(cls, code, date):
        """
        :param code: stock code
        :param date: datetime date
        :return: bool
        """
        return database_session.query(exists().where(and_(cls.StockCode == code,
                                                          cls.RecordDate == date))).scalar()

    @classmethod
    def update(cls, code, date, **kwargs):
        database_session.query(cls).filter(and_(cls.StockCode == code, cls.RecordDate == date)).update(kwargs)
        database_session.commit()


class Stock(Base):
    __tablename__ = 'stock'
    __table_args__ = {'schema': 'hkex'}

    Id = Column(BigInteger, primary_key=True, server_default=text("nextval('hkex.\"stock_Id_seq\"'::regclass)"))
    StockCode = Column(Text, nullable=False)
    StockNameCht = Column(Text)
    StockName = Column(Text)
    Ticker = Column(Text)
    UpdateDate = Column(Date, nullable=False)

    @classmethod
    def exist(cls, code, date):
        """
        :param code: stock code
        :param date: datetime date
        :return: bool
        """
        return database_session.query(exists().where(and_(cls.StockCode == code,
                                                          cls.UpdateDate == date))).scalar()
