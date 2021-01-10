#encoding: UTF-8

from .utilities import \
    INDEX_KEY, DATE_FORMAT, CLOSE_PRICE_NAME, load_vix_by_csv
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
    HTTP_DOWNLOAD_SHFE = 2
    HTTP_DOWNLOAD_DCE = 3
    HTTP_DOWNLOAD_CZCE = 4
    # 沪深 300 指数
    HTTP_DOWNLOAD_CSINDEX_000300 = 5


FIX_FILE_PATTERN = re.compile(r'\^|\=')


#----------------------------------------------------------------------
def get_content_json(response):
    return json.loads(response.text)


#----------------------------------------------------------------------
def get_content_soup(response):
    return BeautifulSoup(response.text, 'html.parser')


#----------------------------------------------------------------------
class IRemoteHttpData(metaclass = ABCMeta):

    remote_path = ""

    def __init__(self, data_path: str, local: str, dates: list):
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
            df = load_vix_by_csv(self.get_local_path())
            return df.index[-1], df
        except (FileNotFoundError, IndexError):
            return None, None

    #----------------------------------------------------------------------
    def sync_data(self):
        """sync the data if needed. """
        try:
            data = self.do_sync_data()
            logger.info(f'{self.get_local_path()} downloaded. ')
            return data
        except (http.client.RemoteDisconnected,
                urllib.error.URLError,
                urllib.error.HTTPError,
                urllib3.exceptions.MaxRetryError,
                requests.exceptions.ConnectionError):
            # for network error handling
            # logger.error(f'{self.remote_path} download failed: {traceback.format_exc()}')
            logger.error(f'{self.remote_path} download failed: {traceback.format_exc(limit = 0)}')
        except:
            logger.error(f'{self.remote_path} download failed: {traceback.format_exc()}')

    #----------------------------------------------------------------------
    def do_sync_data(self):
        """sync the data"""
        li, ldf = self.get_last_index()
        raw_data = self.do_query_remote(li)
        data = self.do_data_handle(raw_data)
        data.index.rename(INDEX_KEY, inplace = True)
        print(data)
        # with index
        if ldf is None:
            data.to_csv(path_or_buf = self.get_local_path())
        else:
            # append data to the local path, this is not work due to the last
            # row is changed from time to time
            # data.to_csv(path_or_buf = self.get_local_path(), mode = 'a', header = False)
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
            logger.error(f"http requests failed. {traceback.format_exc(limit = 0)}")
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

    one_month = "1%E4%B8%AA%E6%9C%88"
    three_years = "3%E5%B9%B4"

    #----------------------------------------------------------------------
    def get_remote_path(self, date_str: str):
        """quety the remote data"""
        if date_str is None:
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
class RemoteHttpCFFEData(IRemoteHttpData):

    # http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0
    remote_path = "http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0"


    #----------------------------------------------------------------------
    def fix_data_index(self, data):
        data.index = data.index.strftime(DATE_FORMAT)


#----------------------------------------------------------------------
class RemoteDataFactory():

    data_path = ''

    def __init__(self, data_path: str):
        """Constructor"""
        self.data_path = data_path

    #----------------------------------------------------------------------
    def create(self, local: str, via: SYNC_DATA_MODE, dates: pd.DataFrame):
        """the creator of RemoteData"""
        data_class = None
        if SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300 == via:
            data_class = RemoteHttpCSIndex000300Data
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE == via:
            data_class = RemoteHttpCFFEData
        if data_class != None:
            return data_class(self.data_path, local, dates)
        raise NotImplementedError
