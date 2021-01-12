#encoding: UTF-8

from .utilities import \
    INDEX_KEY, DATE_FORMAT, CLOSE_PRICE_NAME, check_date_in, load_futures_by_csv
from .xml_to_pandas_dataframe import xml_to_pandas_dataframe
from .logger import logger

from abc import abstractclassmethod, ABCMeta
from enum import Enum
import os, re, json, traceback, urllib, urllib3, requests, http, time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import dateutil.parser as date_parser
import pandas as pd


#----------------------------------------------------------------------
class SYNC_DATA_MODE(Enum):
    HTTP_DOWNLOAD_CFFE = 1
    # trading calendar
    HTTP_DOWNLOAD_CFFE_CALENDAR = 101
    HTTP_DOWNLOAD_SHFE = 2
    HTTP_DOWNLOAD_DCE = 3
    HTTP_DOWNLOAD_CZCE = 4
    # 沪深 300 指数
    HTTP_DOWNLOAD_CSINDEX_000300 = 5


#----------------------------------------------------------------------
class CSV_WRITE_MODE(Enum):
    APPEND = 1
    MERGE = 2


FIX_FILE_PATTERN = re.compile(r'\^|\=')


#----------------------------------------------------------------------
def get_content_json(response):
    return json.loads(response.text)


#----------------------------------------------------------------------
def get_content_soup(response):
    return BeautifulSoup(response.text, 'html.parser')


#----------------------------------------------------------------------
def get_content_xml(response, columns: list, row_name: str):
    df = xml_to_pandas_dataframe(response.text, columns, row_name)
    return df


#----------------------------------------------------------------------
class IRemoteHttpData(metaclass = ABCMeta):

    remote_path = ""
    csv_mode = CSV_WRITE_MODE.APPEND

    def __init__(self, data_path: str, local: str, dates: pd.Index):
        """Constructor"""
        self.data_path = data_path
        self.local = self.fix_file_name(local)
        self.dates = dates

    #----------------------------------------------------------------------
    def fix_file_name(self, local: str):
        """fix the local name"""
        res = FIX_FILE_PATTERN.subn('', local)
        return res[0]

    #----------------------------------------------------------------------
    def get_local_file(self):
        """get local file name"""
        return f'{self.local}.csv'

    #----------------------------------------------------------------------
    def get_local_path(self):
        """get the local file path"""
        return os.path.join(self.data_path, self.get_local_file())

    #----------------------------------------------------------------------
    def get_last_index(self):
        """get the local last index"""
        try:
            df = load_futures_by_csv(self.get_local_path())
            return df.index[-1], df
        except (FileNotFoundError, IndexError):
            return None, None

    #----------------------------------------------------------------------
    def sync_data(self):
        """sync the data if needed. """
        try:
            li, ldf = self.get_last_index()
            dates = self.get_the_request_dates(li)
            data = self.do_sync_data(dates, ldf)
            logger.info(f'{self.get_local_path()} downloaded. ')
            return data
        except (http.client.RemoteDisconnected,
                urllib.error.URLError,
                urllib.error.HTTPError,
                urllib3.exceptions.MaxRetryError,
                requests.exceptions.ConnectionError):
            # for network error handling
            # logger.error(f'{self.remote_path} download failed: {traceback.format_exc()}')
            logger.error(f'sync data failed: {traceback.format_exc(limit = 0)}')
        except:
            logger.error(f'sync data failed: {traceback.format_exc()}')

    #----------------------------------------------------------------------
    def get_the_request_dates(self, local_date: str):
        """get the request date according to the local date and trade dates"""
        if local_date is None:
            return self.dates
        else:
            return self.dates[self.dates > local_date]

    #----------------------------------------------------------------------
    def do_sync_data(self, dates: list, ldf: pd.DataFrame):
        """sync the data"""
        from .data_manager import calendar_manager
        for date in dates:
            if calendar_manager.check_closed(date):
                continue
            ldf = self.do_sync_data_one_by_one(date, ldf)
            time.sleep(0.5)
        return ldf

    #----------------------------------------------------------------------
    def do_sync_data_one_by_one(self, request_date, ldf: pd.DataFrame):
        """request one"""
        raw_data = self.do_query_remote(request_date)
        data = self.do_data_handle(raw_data)
        data.index.rename(INDEX_KEY, inplace = True)
        print(data)
        # with index
        if ldf is None:
            data.to_csv(path_or_buf = self.get_local_path())
        else:
            if CSV_WRITE_MODE.APPEND == self.csv_mode:
                self.do_append_data_to_csv(ldf, data)
            elif CSV_WRITE_MODE.MERGE == self.csv_mode:
                # update the data
                data = self.do_merge_data_to_csv(ldf, data)
        return data

    #----------------------------------------------------------------------
    def do_append_data_to_csv(self, ldf: pd.DataFrame, data: pd.DataFrame):
        """append the data into"""
        data.to_csv(path_or_buf = self.get_local_path(), mode = 'a', header = False)

    #----------------------------------------------------------------------
    def do_merge_data_to_csv(self, ldf: pd.DataFrame, data: pd.DataFrame):
        """merge the data into"""
        # append data to the local path, this is not work due to the last
        # row is changed from time to time
        data = pd.concat([ldf, data])
        # drop the duplicated index rows
        data = data[~data.index.duplicated(keep = 'last')]
        data.to_csv(path_or_buf = self.get_local_path())
        return data

    #----------------------------------------------------------------------
    def do_query_remote(self, date_str: str):
        """query the remote data"""
        url = self.get_remote_path(date_str)
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"http requests failed. {url} {traceback.format_exc(limit = 0)}")
            return False

    #----------------------------------------------------------------------
    def get_remote_path(self, date_str: str):
        """get the http request string"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def do_data_handle(self, data):
        raise NotImplementedError


#----------------------------------------------------------------------
class RemoteHttpCSIndex000300Data(IRemoteHttpData):

    remote_path = "http://www.csindex.com.cn/zh-CN/indices/index-detail/000300?earnings_performance=%s&data_type=json"
    csv_mode = CSV_WRITE_MODE.MERGE

    one_month = "1%E4%B8%AA%E6%9C%88"
    three_years = "3%E5%B9%B4"

    #----------------------------------------------------------------------
    def do_sync_data(self, request_dates: pd.Index, ldf: pd.DataFrame):
        """do sync data at one time"""
        if request_dates.empty:
            return ldf
        else:
            return self.do_sync_data_one_by_one(request_dates, ldf)

    #----------------------------------------------------------------------
    def get_remote_path(self, dates: pd.Index):
        """quety the remote data"""
        if dates.size > 15:
            return self.remote_path % self.three_years
        else:
            return self.remote_path % self.one_month

    #----------------------------------------------------------------------
    def do_data_handle(self, data):
        """"""
        data_list = get_content_json(data)
        df = pd.json_normalize(data_list)
        df.drop(['indx_code', 'changes'], axis = 1, inplace = True)
        df.set_index(['tradedate'], inplace = True)
        df.index = df.index.str.replace(' 00:00:00', '')
        df.rename(columns = {'tclose' : CLOSE_PRICE_NAME}, inplace = True)
        return df


#----------------------------------------------------------------------
class RemoteHttpCFFETradingCalendar(IRemoteHttpData):

    # http://www.cffex.com.cn/sj/jyrl/202011/index_6782.xml
    remote_path = "http://www.cffex.com.cn/sj/jyrl/%s/index_6782.xml"
    csv_mode = CSV_WRITE_MODE.MERGE

    #----------------------------------------------------------------------
    def get_remote_path(self, date: str):
        """query the remote data"""
        str_list = date.split('-')
        year_month = str_list[0] + str_list[1]
        return self.remote_path % year_month

    #----------------------------------------------------------------------
    def do_data_handle(self, data):
        """"""
        df = get_content_xml(data, ['pubdate', 'title'], 'doc')
        df.set_index(['pubdate'], inplace = True)
        df['closed'] = df['title'].str.find(u'休市') != -1
        return df


#----------------------------------------------------------------------
class RemoteHttpCFFEData(IRemoteHttpData):

    # http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0
    remote_path = "http://www.cffex.com.cn/sj/hqsj/rtj/%s/%s/index.xml?id=%d"
    id_request = 0

    #----------------------------------------------------------------------
    def get_remote_path(self, date: str):
        """query the remote data"""
        str_list = date.split('-')
        year_month = str_list[0] + str_list[1]
        day = str_list[2]
        self.id_request += 1
        return self.remote_path % (year_month, day, self.id_request)

    #----------------------------------------------------------------------
    def do_data_handle(self, data):
        """"""
        df = get_content_xml(
            data,
            ['instrumentid', 'tradingday', 'openprice', 'highestprice', 'lowestprice', 'closeprice',
             'preopeninterest', 'openinterest', 'presettlementprice',
             'settlementpriceif', 'settlementprice',
             'volume', 'turnover', 'productid', 'delta', 'expiredate'],
            'dailydata')
        df.set_index(['tradingday'], inplace = True)
        # format the trade day
        index = df.index.str.slice_replace(6, stop = 6, repl = '-')
        df.index = index.str.slice_replace(4, stop = 4, repl = '-')
        df.rename(columns = {'closeprice': CLOSE_PRICE_NAME}, inplace = True)
        return df


#----------------------------------------------------------------------
class RemoteDataFactory():

    data_path = ''

    def __init__(self, data_path: str):
        """Constructor"""
        self.data_path = data_path

    #----------------------------------------------------------------------
    def create(self, local: str, via: SYNC_DATA_MODE, dates: pd.Index):
        """the creator of RemoteData"""
        data_class = None
        if SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300 == via:
            data_class = RemoteHttpCSIndex000300Data
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE == via:
            data_class = RemoteHttpCFFEData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_CALENDAR == via:
            data_class = RemoteHttpCFFETradingCalendar
        if data_class != None:
            return data_class(self.data_path, local, dates)
        raise NotImplementedError
