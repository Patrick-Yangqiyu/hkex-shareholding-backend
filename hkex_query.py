# encoding:utf-8
import numpy as np
import datetime
import io
import logging
import os
import time
import re
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from fake_useragent import UserAgent
from dateutil import parser

import pandas

from models import Snapshot

logger = logging.getLogger(__name__)


def get_CCASS_stock_list(date_str):
    """
    http://www.hkexnews.hk/sdw/search/partlist_c.aspx?SortBy=PartID&ShareholdingDate=20210128
    :return: DataFrame
    """
    url = f"https://www.hkexnews.hk/sdw/search/stocklist.aspx?sortby=stockcode&shareholdingdate={date_str}"
    session = HTMLSession()
    response = session.get(url)

    if response.status_code == 200:
        html = response.html
        table = html.find(".table")
        table_data = pandas.read_html(str(table[0].html), skiprows=0, na_values=None, converters={0: str})
        return table_data[0]
    elif response.status_code == 404:
        return None
    else:
        logger.info("request error", url)


def get_CCASS_participant_list(date_str):
    """
    http://www.hkexnews.hk/sdw/search/partlist.aspx?SortBy=PartID&ShareholdingDate=20210128
    :return: DataFrame
    """
    url = f"http://www.hkexnews.hk/sdw/search/partlist.aspx?SortBy=PartID&ShareholdingDate={date_str}"
    session = HTMLSession()
    response = session.get(url)
    if response.status_code == 200:
        html = response.html
        table = html.find(".table")
        table_data = pandas.read_html(str(table[0].html), skiprows=0, na_values=None)

        return table_data[0]
    elif response.status_code == 404:
        return None
    else:
        logger.info("request error", url)


def get_CCASS_stock_holding_detail_and_snapshot(code, date):
    """
    http://www.hkexnews.hk/sdw/search/searchsdw.aspx
    find the CCASS data and the CCASS snapshot
    :param code:
    :param date:
    :return: dataframe(ccass snapshot), dataframe(holding data), or None, None
    """
    if isinstance(date, str):
        date = parser.parse(date).date()

    ua = UserAgent()

    url = "https://www.hkexnews.hk/sdw/search/searchsdw.aspx"
    session = HTMLSession()
    response = session.get(url)
    if response.status_code == 200:
        html = response.html

        view_state = html.find("#__VIEWSTATE")[0].attrs.get('value')
        view_stage_generator = html.find("#__VIEWSTATEGENERATOR")[0].attrs.get('value')
        event_argument = html.find("#__EVENTARGUMENT")[0].attrs.get('value')

        today = datetime.date.today()
        headers = {"Host": "www.hkexnews.hk",
                   "Origin": "http://www.hkexnews.hk",
                   "Connection": "close",
                   "User-Agent": ua.random,
                   "Referer": url}

        data = {"today": today.strftime("%Y%m%d"),
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": view_stage_generator,
                "__EVENTARGUMENT": event_argument,
                "txtStockCode": code,
                "__EVENTTARGET": "btnSearch",
                "txtShareholdingDate": date.strftime("%Y/%m/%d")
                }

        stock_request = session.post(url, headers=headers, data=data)

        if stock_request.status_code == 200:

            html = stock_request.html
            detail = html.find("#pnlResultNormal")[0]

            detail_table = detail.find("table")

            snapshot = html.find("#pnlResultSummary")[0]

            snapshot_table = snapshot.find(".ccass-search-summary-table")

            if snapshot_table:
                snapshot = Snapshot()
                snapshot.StockCode = code
                snapshot.RecordDate = date

                for data_summary in snapshot_table[0].xpath("//div[contains(@class,'ccass-search-datarow')]"):
                    if data_summary.find('.summary-category')[0].text == 'Market Intermediaries':
                        mi_shareholding = data_summary.xpath("//div[@class='shareholding']/div[@class='value']")[0].text
                        mi_shareholding = mi_shareholding.replace(',', '')

                        if mi_shareholding.isnumeric():
                            mi_shareholding = int(mi_shareholding)
                        else:
                            mi_shareholding = None
                        mi_participant_no = data_summary.xpath("//div[@class='number-of-participants']/div["
                                                               "@class='value']")[0].text

                        mi_participant_no = mi_participant_no.replace(',', '')
                        if mi_participant_no.isnumeric():
                            mi_participant_no = int(mi_participant_no)
                        else:
                            mi_participant_no = None

                        mi_percentage = data_summary.xpath("//div[@class='percent-of-participants']/div["
                                                           "@class='value']")[0].text
                        mi_percentage = mi_percentage.replace('%', '')

                        if mi_percentage.isnumeric():
                            mi_percentage = float(mi_percentage)
                        else:
                            mi_percentage = None

                        snapshot.MarketIntermediariesShareholding = mi_shareholding
                        snapshot.MarketIntermediariesParticipantNum = mi_participant_no
                        snapshot.MarketIntermediariesPercentage = mi_percentage
                    elif data_summary.find('.summary-category')[0].text == 'Consenting Investor Participants':
                        ci_shareholding = data_summary.xpath("//div[@class='shareholding']/div[@class='value']")[0].text
                        ci_shareholding = ci_shareholding.replace(',', '')

                        if ci_shareholding.isnumeric():
                            ci_shareholding = int(ci_shareholding)
                        else:
                            ci_shareholding = None

                        ci_participant_no = data_summary.xpath("//div[@class='number-of-participants']/div["
                                                               "@class='value']")[0].text
                        ci_participant_no = ci_participant_no.replace(',', '')
                        if ci_participant_no.isnumeric():
                            ci_participant_no = int(ci_participant_no)
                        else:
                            ci_participant_no = None

                        ci_percentage = data_summary.xpath("//div[@class='percent-of-participants']/div["
                                                           "@class='value']")[0].text
                        ci_percentage = ci_percentage.replace('%', '')

                        if ci_percentage.isnumeric():
                            ci_percentage = float(ci_percentage)
                        else:
                            ci_percentage = None
                        snapshot.ConsentingInvestorNum = ci_participant_no
                        snapshot.ConsentingInvestorPercentage = ci_percentage
                        snapshot.ConsentingInvestorShareholding = ci_shareholding

                    elif data_summary.find('.summary-category')[0].text == 'Non-consenting Investor Participants':
                        nci_shareholding = data_summary.xpath("//div[@class='shareholding']/div[@class='value']")[
                            0].text
                        nci_shareholding = nci_shareholding.replace(',', '')

                        if nci_shareholding.isnumeric():
                            nci_shareholding = int(nci_shareholding)
                        else:
                            nci_shareholding = None

                        nci_participant_no = data_summary.xpath("//div[@class='number-of-participants']/div["
                                                                "@class='value']")[0].text
                        nci_participant_no = nci_participant_no.replace(',', '')
                        if nci_participant_no.isnumeric():
                            nci_participant_no = int(nci_participant_no)
                        else:
                            nci_participant_no = None

                        nci_percentage = data_summary.xpath("//div[@class='percent-of-participants']/div["
                                                            "@class='value']")[0].text
                        nci_percentage = nci_percentage.replace('%', '')

                        if nci_percentage.isnumeric():
                            nci_percentage = float(nci_percentage)
                        else:
                            nci_percentage = None

                        snapshot.NonConsentingInvestorNum = nci_participant_no
                        snapshot.NonConsentingInvestorPercentage = nci_percentage
                        snapshot.NonConsentingInvestorShareholding = nci_shareholding

                    elif data_summary.find('.summary-category')[0].text == 'Total':
                        total_shareholding = data_summary.xpath("//div[@class='shareholding']/div[@class='value']")[
                            0].text
                        total_shareholding = total_shareholding.replace(',', '')

                        if total_shareholding.isnumeric():
                            total_shareholding = int(total_shareholding)
                        else:
                            total_shareholding = None

                        total_participant_no = data_summary.xpath("//div[@class='number-of-participants']/div["
                                                                  "@class='value']")[0].text
                        total_participant_no = total_participant_no.replace(',', '')
                        if total_participant_no.isnumeric():
                            total_participant_no = int(total_participant_no)
                        else:
                            total_participant_no = None

                        total_percentage = data_summary.xpath("//div[@class='percent-of-participants']/div["
                                                              "@class='value']")[0].text
                        total_percentage = total_percentage.replace('%', '')

                        if total_percentage.isnumeric():
                            total_percentage = float(total_percentage)
                        else:
                            total_percentage = None

                        snapshot.TotalShareholding = total_shareholding
                        snapshot.TotalNum = total_participant_no
                        snapshot.TotalPercentage = total_percentage
                total_issued = \
                    snapshot_table[0].xpath("//div[@class='ccass-search-remarks']/div[@class='summary-value']")[0].text
                total_issued = total_issued.replace(',', '')
                if total_issued.isnumeric():
                    total_issued = int(total_issued)
                else:
                    total_issued = None

            else:
                snapshot = None

            if detail_table:
                detail_data = pandas.read_html(detail_table[0].html, keep_default_na=True)
                detail_data = detail_data[0]

                detail_data['ParticipantCode'] = detail_data['Participant ID'].str.split(':').str[1]
                detail_data['ParticipantName'] = detail_data[
                    'Name of CCASS Participant(* for Consenting Investor Participants )'].str.split(':').str[1]
                detail_data['Address'] = detail_data['Address'].str.split(':').str[1]
                detail_data['Shareholding'] = detail_data['Shareholding'].str.split(':').str[1]
                detail_data['Shareholding'] = detail_data['Shareholding'].str.replace(',', '').astype(np.int64)
                detail_data['Percentage'] = detail_data.iloc[:, 4].str.split(':').str[1]
                detail_data['Percentage'] = detail_data['Percentage'].str.replace(
                    '%', '').astype(float)
                detail_data['StockCode'] = code
                detail_data['RecordDate'] = date
                detail_data = detail_data[[
                    'ParticipantCode',
                    'StockCode',
                    'Shareholding',
                    'Percentage',
                    'RecordDate',
                    'ParticipantName',
                    'Address'
                ]]
            else:
                detail_data = None
            return snapshot, detail_data
        elif stock_request.status_code == 404:
            return None, None
        else:
            logger.error("request error", url, "post data=>", data)
    else:
        logger.error("request error", url)


