from flask import Flask, jsonify
from flask_restful import Resource, Api, abort, reqparse
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

from models import Detail

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

from database import db_string

app.config['SQLALCHEMY_DATABASE_URI'] = db_string
db = SQLAlchemy(app)
api = Api(app)

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('stock_code', type=str, required=True)
parser.add_argument('start_date', type=str, required=True)
parser.add_argument('end_date', type=str, required=True)
parser.add_argument('threshold', type=float)


class Trend(Resource):
    def post(self):
        args = parser.parse_args()
        stock_code = args['stock_code']
        start_date = args['start_date']
        end_date = args['end_date']

        sql = '''
                    SELECT *
                     from hkex.detail
                     where hkex.detail."ParticipantName" in (
                         SELECT detail_rank."ParticipantName"
                         from (SELECT hkex.detail.*,
                                      rank() OVER (
                                          PARTITION BY hkex.detail."RecordDate" , hkex.detail."StockCode"
                                          ORDER BY hkex.detail."Shareholding" DESC
                                          )
                               FROM hkex.detail
                              where  hkex.detail."RecordDate" = to_date(:enddate, 'YYYYMMDD')
                             ) detail_rank
                         where detail_rank.rank <= 10
                           and detail_rank."StockCode" = :stockcode
                     )
                       and hkex.detail."StockCode" = :stockcode
                       and hkex.detail."RecordDate" >= to_date(:startdate, 'YYYYMMDD') AND hkex.detail."RecordDate" <= to_date(:enddate, 'YYYYMMDD');

        '''
        stmt = text(sql)
        result = db.session.query(Detail).from_statement(stmt).params(startdate=start_date, enddate=end_date,
                                                                      stockcode=stock_code).all()
        return [r.toJSON for r in result]


class Transaction(Resource):
    def post(self):
        args = parser.parse_args()
        stock_code = args['stock_code']
        start_date = args['start_date']
        end_date = args['end_date']
        threshold = args['threshold']
        sql = '''
                select T."Id", TRIM(T."ParticipantCode") as "ParticipantCode", T."StockCode", T."Shareholding", T."Percentage", to_char(T."RecordDate",'YYYY-MM-DD') as "RecordDate" , TRIM(T."ParticipantName") as "ParticipantName", T."Address",
                 T."Shareholding" - COALESCE(Tminus1."Shareholding",0) as "DiffShs", T."Percentage" - COALESCE(Tminus1."Percentage",0) as "DiffPercentage"
                from hkex.detail T
                      left join hkex.detail Tminus1
                                    on T."StockCode" = Tminus1."StockCode"
                                           and T."ParticipantCode" = Tminus1."ParticipantCode"
                                           and T."ParticipantName" = Tminus1."ParticipantName"
                                           and (T."RecordDate" - Tminus1."RecordDate") = 1
                WHERE T."StockCode" = :stockcode
                and  T."RecordDate"  >= to_date(:startdate, 'YYYYMMDD')  AND T."RecordDate" <= to_date(:enddate, 'YYYYMMDD')
                and ABS(T."Percentage" - COALESCE(Tminus1."Percentage",0) ) >= :threshold
                ;
                  '''
        stmt = text(sql)
        query = db.session.execute(stmt, {"startdate": start_date, "enddate": end_date, "stockcode": stock_code,
                                          "threshold": threshold})
        return [dict(row) for row in query]


api.add_resource(Trend, '/trend')
api.add_resource(Transaction, '/transaction')
if __name__ == '__main__':
    app.run(debug=True)
