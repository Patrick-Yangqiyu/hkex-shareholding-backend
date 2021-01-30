from datetime import datetime

from dateutil import parser
import time
import logging

import hkex_query
from models import Stock, Participant, Snapshot, Detail

from database import database_session, db
log_name = datetime.now().strftime('mylogfile_%Y-%m-%d_%H-%M-%S.log')

logging.basicConfig(filename=log_name, filemode='w', format='%(asctime)s %(filename)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

run_date = '20210123'
today = parser.parse(run_date).date()

date_str = today.strftime("%Y%m%d")

stock_list_df = hkex_query.get_CCASS_stock_list(date_str)
if stock_list_df is not None and not stock_list_df.empty:
    stock_model_list = []
    for index, row in stock_list_df.iterrows():
        stock_code = row[0]
        stock_name = row[1]
        if not Stock.exist(stock_code, today):
            stock_model = Stock()
            stock_model.StockCode = stock_code
            stock_model.StockName = stock_name
            stock_model.UpdateDate = today
            stock_model_list.append(stock_model)
    database_session.bulk_save_objects(stock_model_list)
    database_session.commit()

participant_list_df = hkex_query.get_CCASS_participant_list(date_str)
if participant_list_df is not None and not participant_list_df.empty:
    participant_model_list = []
    participant_list_df = participant_list_df.fillna('NO ID')
    for index, row in participant_list_df.iterrows():
        participant_code = row[0]
        participant_name = row[1]
        if not Participant.exist(participant_code, participant_name, today):
            participant_model = Participant()
            participant_model.ParticipantCode = participant_code
            participant_model.ParticipantName = participant_name
            participant_model.UpdateDate = today
            participant_model_list.append(participant_model)
    database_session.bulk_save_objects(participant_model_list)
    database_session.commit()

subquery = database_session.query(Detail.StockCode).filter(Detail.RecordDate == today).subquery()
query = database_session.query(Stock).filter(Stock.UpdateDate == today).filter(~Stock.StockCode.in_(subquery))


for stock in query.all():
    time.sleep(1)
    logger.debug(f"STOCK CODE :{stock.StockCode} , Date:{date_str}")
    try:
        snapshot, detail_data = hkex_query.get_CCASS_stock_holding_detail_and_snapshot(stock.StockCode, today)
        if not Snapshot.exist(stock.StockCode, today):
            database_session.add(snapshot)
        for index, row in detail_data.iterrows():
            detail_model_list = []
            if not Detail.exist(stock.StockCode, today, row['ParticipantCode'], row['ParticipantName']):
                detail_model = Detail()
                detail_model.ParticipantCode = row['ParticipantCode']
                detail_model.ParticipantName = row['ParticipantName']
                detail_model.StockCode = row['StockCode']
                detail_model.Percentage = row['Percentage']
                detail_model.RecordDate = row['RecordDate']
                detail_model.Address = row['Address']
                detail_model.Shareholding = row['Shareholding']
                detail_model_list.append(detail_model)
            else:
                Detail.update(stock.StockCode, today, row['ParticipantCode'], row['ParticipantName'], **{
                    "Percentage": row["Percentage"],
                    "Address": row["Address"],
                    "Shareholding": row["Shareholding"]
                })

            database_session.bulk_save_objects(detail_model_list)
            database_session.commit()
    except Exception as e:
        logger.exception(e)
        logger.debug(f"ERROR on - STOCK CODE :{stock.StockCode} , Date:{date_str}")
        continue  # or you could use 'continue'
