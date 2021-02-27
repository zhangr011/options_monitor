#encoding: UTF-8


from .data_ref import SYNC_DATA_MODE, DATA_ROOT, make_sure_dirs_exist
from .data_ref import INDEX_KEY, DATE_FORMAT
from .data_ref import PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, \
    PRE_SETTLE_PRICE_NAME, SETTLE_PRICE_NAME, OPEN_INTEREST_NAME, OI_CHG_NAME, \
    VOLUME_NAME, TURNOVER_NAME, TOTAL_ROW_KEY, COLUMN_NAMES
from .data_ref import IV_NAME, U_PRODUCT_ID_NAME, S_PRICE_NAME, \
    U_PRICE_NAME, OPTION_TYPE_NAME, O_COLUMN_NAMES, EXPIRY_NAME
from .utilities import load_futures_by_csv
from .utilities_options import oc_mgr
from .xml_to_pandas_dataframe import xml_to_pandas_dataframe
from .soup_to_pandas_dataframe import soup_to_pandas_dataframe
from .singleton import Singleton
from .logger import logger

from abc import abstractclassmethod, ABCMeta
from enum import Enum
import os, re, json, traceback, urllib, urllib3, requests, http, time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


#----------------------------------------------------------------------
class CSV_WRITE_MODE(Enum):
    APPEND = 1
    MERGE = 2


FIX_FILE_PATTERN = re.compile(r'\^|\=')


GB_ENCODING = 'gb18030'


# for siv
STRIKE_BIAS_NAME = 'sbias'
OPTION_BIAS_AA = 0.25

OPTIONS_NAME_RE = '(\w+\d+)(C|P)(\d+)'
OPTIONS_NAME_DASH_RE = '(\w+\d+)-(C|P)-(\d+)'


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    if proxy is None:
        return
    requests.get("http://127.0.0.1:5010/delete/?proxy={proxy}")


#----------------------------------------------------------------------
def get_content_json(response):
    return json.loads(response.text)


#----------------------------------------------------------------------
def get_content_soup(response, encoding: str = None):
    return BeautifulSoup(response.content, 'html.parser', from_encoding = encoding)


#----------------------------------------------------------------------
def get_table_soup(response, encoding: str = None, thead_tbody: bool = True):
    soup = get_content_soup(response, encoding)
    df = soup_to_pandas_dataframe(soup, thead_tbody)
    return df


#----------------------------------------------------------------------
def get_content_xml(response, columns: list, row_name: str):
    df = xml_to_pandas_dataframe(response.text, columns, row_name)
    return df


#----------------------------------------------------------------------
def to_numeric(df: pd.DataFrame, column: str, to_type: type = float):
    """"""
    # parse price
    try:
        to = df[column].astype(to_type)
    except ValueError:
        to = df[column].str.replace('^-$', '0', regex = True)
        to = to.str.replace(',', '', regex = True).astype(to_type)
    df[column] = to
    return df


#----------------------------------------------------------------------
def normalize_total_key(df: pd.DataFrame, key: str):
    """"""
    df[PRODUCT_ID_NAME] = df[PRODUCT_ID_NAME].str.replace(key, TOTAL_ROW_KEY, regex = True)
    return df


#----------------------------------------------------------------------
def fill_total_keys(df: pd.DataFrame):
    """"""
    keys = df.groupby([df.index, PRODUCT_GROUP_NAME]).groups.keys()
    rows = []
    for date, key in keys:
        row = df[(df.index == date) & (df[PRODUCT_GROUP_NAME] == key)].iloc[0]
        row[PRODUCT_ID_NAME] = TOTAL_ROW_KEY
        rows.append(row)
    return df.append(rows)


#----------------------------------------------------------------------
def normalize_history_data(df: pd.DataFrame, final_key: str = u'总计'):
    """"""
    # drop unexpected columns
    df = df[COLUMN_NAMES]
    # clear the final rows
    df = df[(df[PRODUCT_ID_NAME] != final_key) & (df[PRODUCT_GROUP_NAME] != final_key)]
    df[PRODUCT_GROUP_NAME] = df[PRODUCT_GROUP_NAME].str.strip()
    # clear empty open interest
    df = df[df[OPEN_INTEREST_NAME].notnull()]
    df.replace('', '0', regex = True, inplace = True)
    df = to_numeric(df, OPEN_INTEREST_NAME, int)
    df = df[df[OPEN_INTEREST_NAME] > 0]
    # normalize price
    df = to_numeric(df, CLOSE_PRICE_NAME)
    df = to_numeric(df, PRE_SETTLE_PRICE_NAME)
    df = to_numeric(df, SETTLE_PRICE_NAME)
    df = to_numeric(df, OPEN_PRICE_NAME)
    df = to_numeric(df, HIGH_PRICE_NAME)
    df = to_numeric(df, LOW_PRICE_NAME)
    df = to_numeric(df, OI_CHG_NAME, int)
    df = to_numeric(df, VOLUME_NAME, int)
    return df


#----------------------------------------------------------------------
def normalize_options_data(df: pd.DataFrame, final_key: str = u'总计'):
    """"""
    # drop unexpected columns
    df = df[O_COLUMN_NAMES]
    # clear the final rows
    df = df[(df[PRODUCT_ID_NAME] != final_key) & (df[PRODUCT_GROUP_NAME] != final_key)]
    df.replace('', '0', regex = True, inplace = True)
    df = df[df[VOLUME_NAME].notnull()]
    df = to_numeric(df, VOLUME_NAME, int)
    # clear empty volume, 'total' key must be remained
    df = df[(df[VOLUME_NAME] > 0) | (df[PRODUCT_ID_NAME] == TOTAL_ROW_KEY)]
    df = to_numeric(df, OPEN_INTEREST_NAME, int)
    # normalize price
    df = to_numeric(df, S_PRICE_NAME, float)
    df = to_numeric(df, U_PRICE_NAME, float)
    # df = to_numeric(df, IV_NAME, float)
    df = to_numeric(df, CLOSE_PRICE_NAME, float)
    return df


#----------------------------------------------------------------------
def fix_close_data(df: pd.DataFrame):
    """fix czce's close price of 0"""
    df[CLOSE_PRICE_NAME] = np.where(df[CLOSE_PRICE_NAME] == 0,
                                    df[SETTLE_PRICE_NAME],
                                    df[CLOSE_PRICE_NAME])
    return df


#----------------------------------------------------------------------
def calculate_index(df_in: pd.DataFrame, total_key: str = TOTAL_ROW_KEY):
    """calculate the index, weighted average close price by open interest"""
    # https://stackoverflow.com/questions/31521027/groupby-weighted-average-and-sum-in-pandas-dataframe
    # https://stackoverflow.com/questions/26205922/calculate-weighted-average-using-a-pandas-dataframe
    df = df_in[df_in[PRODUCT_ID_NAME] != TOTAL_ROW_KEY]
    wm = lambda x: np.average(x[CLOSE_PRICE_NAME], weights = x[OPEN_INTEREST_NAME])
    group = df.groupby([df.index, PRODUCT_GROUP_NAME]).apply(wm)
    df_in[CLOSE_PRICE_NAME] = np.where(
        df_in[PRODUCT_ID_NAME] == total_key,
        round(group[pd.MultiIndex.from_arrays([df_in.index, df_in[PRODUCT_GROUP_NAME]])], 3),
        df_in[CLOSE_PRICE_NAME])
    return df_in


#----------------------------------------------------------------------
def parse_options_name(df: pd.DataFrame, pattern: str = OPTIONS_NAME_RE):
    df[[U_PRODUCT_ID_NAME, OPTION_TYPE_NAME, S_PRICE_NAME]] = df[PRODUCT_ID_NAME].str.extract(pattern)
    return df


#----------------------------------------------------------------------
def calc_iv_helper(row, total_key: str):
    if row[PRODUCT_ID_NAME] == total_key:
        # for total key, pass the old iv directly
        return 0.
    else:
        iv = oc_mgr.calc_iv(row[PRODUCT_ID_NAME], row[CLOSE_PRICE_NAME], row[U_PRICE_NAME], row.name)
        return iv


#----------------------------------------------------------------------
def calculate_iv(df: pd.DataFrame, total_key: str = TOTAL_ROW_KEY):
    """calculate single product's iv"""
    df[IV_NAME] = df.apply(lambda row: calc_iv_helper(row, total_key), axis = 1)
    return df


#----------------------------------------------------------------------
def calculate_siv(df_in: pd.DataFrame, total_key: str = TOTAL_ROW_KEY):
    """"""
    return calculate_siv_by_turnovers(df_in, total_key)


#----------------------------------------------------------------------
def calculate_siv_by_volumes(df_in: pd.DataFrame, total_key: str = TOTAL_ROW_KEY):
    """according to [options as strategy]"""
    return calculate_siv_by_column(df_in, VOLUME_NAME, total_key)


#----------------------------------------------------------------------
def calculate_siv_by_turnovers(df_in: pd.DataFrame, total_key: str = TOTAL_ROW_KEY):
    """as by volumes, the options witch are due to expire may cause iv inaccurate. """
    return calculate_siv_by_column(df_in, TURNOVER_NAME, total_key)


#----------------------------------------------------------------------
def calculate_siv_by_column(df_in: pd.DataFrame, column: str, total_key: str = TOTAL_ROW_KEY):
    """calculate the siv by the column,  maybe VOLUME_NAME or TURNOVER_NAME"""
    df = df_in[df_in[PRODUCT_ID_NAME] != TOTAL_ROW_KEY]
    # filter the normal iv
    df = df[df[IV_NAME] > 0.01]
    df[STRIKE_BIAS_NAME] = abs(df[S_PRICE_NAME] - df[U_PRICE_NAME]) / df[U_PRICE_NAME]
    df[STRIKE_BIAS_NAME] = np.where(df[STRIKE_BIAS_NAME] > OPTION_BIAS_AA,
                                    0,
                                    np.square((df[STRIKE_BIAS_NAME] - OPTION_BIAS_AA) / OPTION_BIAS_AA))
    # use price * volumes as turnovers
    df[TURNOVER_NAME] = df[CLOSE_PRICE_NAME] * df[VOLUME_NAME]
    # totals = df[[PRODUCT_GROUP_NAME, column]].groupby([df.index, PRODUCT_GROUP_NAME]).sum()
    # df = df.join(totals, how = 'left', on = [df.index, PRODUCT_GROUP_NAME], rsuffix = '_all')
    df['weights'] = df[STRIKE_BIAS_NAME] * df[column]
    wm = lambda x: np.average(x[IV_NAME], weights = x['weights'])
    group = df.groupby([df.index, PRODUCT_GROUP_NAME]).apply(wm)
    df_in[IV_NAME] = np.where(
        df_in[PRODUCT_ID_NAME] == total_key,
        round(group[pd.MultiIndex.from_arrays([df_in.index, df_in[PRODUCT_GROUP_NAME]])], 3),
        df_in[IV_NAME])
    return df_in


#----------------------------------------------------------------------
class IRemoteHttpData(metaclass = ABCMeta):

    remote_path = ""
    csv_mode = CSV_WRITE_MODE.APPEND
    request_post = False
    proxy_mode = False

    def __init__(self, data_path: str, local: str, dates: pd.Index,
                 df_extra: pd.DataFrame):
        """Constructor"""
        self.data_path = data_path
        self.local = self.fix_file_name(local)
        self.dates = dates
        self.df_extra = self.fix_df_extra(df_extra)

    #----------------------------------------------------------------------
    def fix_df_extra(self, df: pd.DataFrame):
        """api for data processing. """
        return df

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
    def drop_local_last_date_data(self):
        """clear the last date data"""
        lindex, df = self.get_last_index()
        if lindex:
            index_names = df[df.index == lindex].index
            df.drop(index = index_names, inplace = True)
            df.to_csv(path_or_buf = self.get_local_path())
            logger.info(f'data of {lindex} has been dropped. ')

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
            logger.error(f'sync data failed {dates}: {traceback.format_exc()}')

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
        for date in dates:
            ldf = self.do_sync_data_one_by_one(date, ldf)
            time.sleep(0.5)
        return ldf

    #----------------------------------------------------------------------
    def do_sync_data_one_by_one(self, request_date, ldf: pd.DataFrame):
        """request one"""
        raw_data = self.do_query_remote(request_date)
        data = self.do_data_handle(raw_data, request_date)
        if data is None:
            return
        data.index.rename(INDEX_KEY, inplace = True)
        # print(data)
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
    def recalculate_iv_test(self):
        """recalculate the iv"""
        _li, df = self.get_last_index()
        df = calculate_iv(df)
        self.save_data_test(df)

    #----------------------------------------------------------------------
    def recalculate_siv_test(self):
        """recalculate the siv"""
        _li, df = self.get_last_index()
        df = calculate_siv(df)
        self.save_data_test(df)

    #----------------------------------------------------------------------
    def save_data_test(self, df: pd.DataFrame):
        """save the data, for test only"""
        df.to_csv(path_or_buf = self.get_local_path())

    #----------------------------------------------------------------------
    def do_query_remote_once(self, url: str, data: dict, hds: dict,
                             proxies: dict, retry_count: int = 0):
        """query the remote data with 5 retry times"""
        if retry_count < 0:
            return False
        try:
            if self.request_post:
                response = requests.post(url, data, headers = hds, proxies = proxies)
            else:
                response = requests.get(url, headers = hds, proxies = proxies)
            if 200 == response.status_code:
                # get the remote info
                return response
            elif 304 == response.status_code:
                # be banned
                logger.error(f'http requests 403 error: {url}')
                return response.status_code
        except Exception as e:
            logger.error(f"http requests failed. {url} {traceback.format_exc(limit = 0)}")
        return self.do_query_remote_once(url, data, hds, proxies, retry_count - 1)

    #----------------------------------------------------------------------
    def do_query_remote_once_with_proxy(self, url: str, data: dict, hds: dict,
                                        proxy: str, proxies: dict, retry_count: int = 0):
        """with proxies"""
        if retry_count < 0:
            return False
        # no proxies at first
        if isinstance(proxy, str):
            proxies = {"http": f"http://{proxy}"}
        else:
            proxies = {}
        result = self.do_query_remote_once(url, data, hds, proxies, 5)
        if 304 == result:
            delete_proxy(proxy)
            proxy = get_proxy().get('proxy')
            # try to use another proxy
            return self.do_query_remote_once_with_proxy(url, data, hds, proxy, retry_count - 1)
        else:
            return result

    #----------------------------------------------------------------------
    def do_query_remote(self, date_str: str):
        """query the remote data"""
        hds = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
        data = None
        if self.request_post:
            url, data = self.get_remote_path(date_str)
        else:
            url = self.get_remote_path(date_str)
        return self.do_query_remote_once_with_proxy(url, data, hds, None, 1000)

    #----------------------------------------------------------------------
    def get_underlying_close_price(self, df: pd.DataFrame, date_str: str):
        """"""
        if self.df_extra is None:
            raise NotImplementedError('underlying dataframe is None')
        day_df = self.df_extra[self.df_extra.index == date_str][[PRODUCT_ID_NAME, CLOSE_PRICE_NAME]]
        day_df.set_index(PRODUCT_ID_NAME, inplace = True)
        df = df.join(day_df, rsuffix = '_u', how = 'left', on = U_PRODUCT_ID_NAME)
        df.rename({'Close_u': U_PRICE_NAME}, axis = 1, inplace = True)
        return df

    #----------------------------------------------------------------------
    def get_remote_path(self, date_str: str):
        """get the http request string"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        raise NotImplementedError

    #----------------------------------------------------------------------
    # def fix_close_data(self):
    #     """"""
    #     li, df = self.get_last_index()
    #     df = fix_close_data(df)
    #     df = calculate_index(df)
    #     df.to_csv(path_or_buf = self.get_local_path())


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
    def do_data_handle(self, data, date_str: str):
        """"""
        data_list = get_content_json(data)
        df = pd.json_normalize(data_list)
        df.drop(['lclose', 'changes'], axis = 1, inplace = True)
        df.set_index(['tradedate'], inplace = True)
        df.index = df.index.str.replace(' 00:00:00', '')
        df.rename(columns = {
            'indx_code' : PRODUCT_GROUP_NAME,
            'tclose' : CLOSE_PRICE_NAME}, inplace = True)
        df[PRODUCT_GROUP_NAME] = 'csidx300'
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
    def do_data_handle(self, data, date_str: str):
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
    def do_data_handle(self, data, date_str: str):
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
        df.rename({
            'instrumentid' : PRODUCT_ID_NAME,
            'productid' : PRODUCT_GROUP_NAME,
            'presettlementprice' : PRE_SETTLE_PRICE_NAME,
            'openprice' : OPEN_PRICE_NAME,
            'highestprice' : HIGH_PRICE_NAME,
            'lowestprice' : LOW_PRICE_NAME,
            'closeprice': CLOSE_PRICE_NAME,
            'settlementprice' : SETTLE_PRICE_NAME,
            'openinterest' : OPEN_INTEREST_NAME,
            'volume' : VOLUME_NAME,
            'expiredate' : EXPIRY_NAME}, axis = 1, inplace = True)
        df[OI_CHG_NAME] = df[OPEN_INTEREST_NAME].astype(int) - df['preopeninterest'].astype(int)
        df = self.do_data_handle_extra(df, date_str)
        return df

    #----------------------------------------------------------------------
    def do_data_handle_extra(self, df: pd.DataFrame, date_str: str):
        """"""
        # filter the options data
        df = df[~df[PRODUCT_GROUP_NAME].isin(['IO'])]
        df = fill_total_keys(df)
        df = normalize_history_data(df)
        df = calculate_index(df)
        return df


#----------------------------------------------------------------------
class RemoteHttpCFFEOptionsData(RemoteHttpCFFEData):

    # http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0
    remote_path = "http://www.cffex.com.cn/sj/hqsj/rtj/%s/%s/index.xml?id=%d"
    id_request = 0

    #----------------------------------------------------------------------
    def fix_df_extra(self, df: pd.DataFrame):
        """api for data processing. """
        df[PRODUCT_ID_NAME] = df[PRODUCT_GROUP_NAME]
        return df

    #----------------------------------------------------------------------
    def do_data_handle_extra(self, df: pd.DataFrame, date_str: str):
        """"""
        df = df[df[PRODUCT_GROUP_NAME].isin(['IO'])]
        if df.empty:
            # options data maybe empty
            return None
        df = fill_total_keys(df)
        df = parse_options_name(df, OPTIONS_NAME_DASH_RE)
        # set the IO options' underlying contract to csindex000300
        df[U_PRODUCT_ID_NAME] = df[U_PRODUCT_ID_NAME].str.replace('IO\d+', 'csidx300', regex = True)
        df2 = self.get_underlying_close_price(df, date_str)
        df2 = normalize_options_data(df2)
        df2 = calculate_iv(df2)
        df2 = calculate_siv(df2)
        return df2


#----------------------------------------------------------------------
class RemoteHttpSHFEData(IRemoteHttpData):

    # http://www.shfe.com.cn/data/dailydata/kx/kx20210105.dat
    remote_path = "http://www.shfe.com.cn/data/dailydata/kx/kx%s.dat"

    #----------------------------------------------------------------------
    def get_remote_path(self, date: str):
        """query the remote data"""
        str_list = date.split('-')
        req_date = str_list[0] + str_list[1] + str_list[2]
        return self.remote_path % req_date

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        data_list = get_content_json(data)
        data_list = data_list.get('o_curinstrument')
        df = pd.json_normalize(data_list)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename(columns = {
            'DELIVERYMONTH' : PRODUCT_ID_NAME,
            'PRODUCTID'   : PRODUCT_GROUP_NAME,
            'PRESETTLEMENTPRICE' : PRE_SETTLE_PRICE_NAME,
            'OPENPRICE'   : OPEN_PRICE_NAME,
            'HIGHESTPRICE': HIGH_PRICE_NAME,
            'LOWESTPRICE' : LOW_PRICE_NAME,
            'CLOSEPRICE'  : CLOSE_PRICE_NAME,
            'SETTLEMENTPRICE': SETTLE_PRICE_NAME,
            'OPENINTEREST': OPEN_INTEREST_NAME,
            'OPENINTERESTCHG': OI_CHG_NAME,
            'VOLUME'      : VOLUME_NAME}, inplace = True)
        # replace the total row's key name
        df = normalize_total_key(df, u'小计')
        # fix the product id
        df[PRODUCT_ID_NAME] = np.where(
            df[PRODUCT_ID_NAME] == TOTAL_ROW_KEY,
            df[PRODUCT_ID_NAME],
            df[PRODUCT_GROUP_NAME].str.strip().str.replace('_f', '') + df[PRODUCT_ID_NAME])
        df = normalize_history_data(df, u'总计')
        df = calculate_index(df)
        return df


#----------------------------------------------------------------------
class RemoteHttpSHFEOptionsData(RemoteHttpSHFEData):

    # http://www.shfe.com.cn/data/dailydata/option/kx/kx20210105.dat
    remote_path = 'http://www.shfe.com.cn/data/dailydata/option/kx/kx%s.dat'

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        data_list = get_content_json(data)
        data_list = data_list.get('o_curinstrument')
        df = pd.json_normalize(data_list)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename({
            'INSTRUMENTID' : PRODUCT_ID_NAME,
            'PRODUCTGROUPID' : PRODUCT_GROUP_NAME,
            'PRESETTLEMENTPRICE' : PRE_SETTLE_PRICE_NAME,
            'OPENPRICE'    : OPEN_PRICE_NAME,
            'HIGHESTPRICE' : HIGH_PRICE_NAME,
            'CLOSEPRICE'   : CLOSE_PRICE_NAME,
            'SETTLEMENTPRICE' : SETTLE_PRICE_NAME,
            'OPENINTEREST' : OPEN_INTEREST_NAME,
            'OPENINTERESTCHG' : OI_CHG_NAME,
            'VOLUME' : VOLUME_NAME}, axis = 1, inplace = True)
        # replace the total row's key name
        df = normalize_total_key(df, u'小计')
        df[PRODUCT_ID_NAME] = df[PRODUCT_ID_NAME].str.strip()
        df[PRODUCT_GROUP_NAME] = df[PRODUCT_GROUP_NAME].str.strip()
        df = parse_options_name(df)
        # drop the final statistics row
        df = df[~df[PRODUCT_ID_NAME].str.contains(u'合计', regex = True)]
        df2 = self.get_underlying_close_price(df, date_str)
        df2 = normalize_options_data(df2)
        df2 = calculate_iv(df2)
        df2 = calculate_siv(df2)
        return df2

#----------------------------------------------------------------------
class RemoteHttpDCEData(IRemoteHttpData):

    # http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html
    remote_path = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"
    request_post = True
    # for futures
    trade_type = '0'
    instruments_map = {
        u'玉米' : 'c',
        u'玉米淀粉' : 'cs',
        u'豆一' : 'a',
        u'豆二' : 'b',
        u'豆粕' : 'm',
        u'豆油' : 'y',
        u'棕榈油' : 'p',
        u'纤维板' : 'fb',
        u'胶合板' : 'bb',
        u'鸡蛋' : 'jd',
        u'粳米' : 'rr',
        u'生猪' : 'lh',
        u'聚乙烯' : 'l',
        u'聚氯乙烯' : 'v',
        u'聚丙烯' : 'pp',
        u'焦炭' : 'j',
        u'焦煤' : 'jm',
        u'铁矿石' : 'i',
        u'乙二醇' : 'eg',
        u'苯乙烯' : 'eb',
        u'液化石油气' : 'pg'
    }

    #----------------------------------------------------------------------
    def get_remote_path(self, date: str):
        """query the remote data"""
        date_list = date.split('-')
        data = {'dayQuotes.variety': 'all',
                'dayQuotes.trade_type': self.trade_type,
                'year': date_list[0],
                'month': str(int(date_list[1]) - 1),
                'day': date_list[2]}
        return self.remote_path, data

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        df = get_table_soup(data, GB_ENCODING, False)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename({
            u'交割月份' : PRODUCT_ID_NAME,
            u'商品名称' : PRODUCT_GROUP_NAME,
            u'前结算价' : PRE_SETTLE_PRICE_NAME,
            u'开盘价'   : OPEN_PRICE_NAME,
            u'最高价'   : HIGH_PRICE_NAME,
            u'最低价'   : LOW_PRICE_NAME,
            u'收盘价'   : CLOSE_PRICE_NAME,
            u'结算价'   : SETTLE_PRICE_NAME,
            u'持仓量'   : OPEN_INTEREST_NAME,
            u'持仓量变化' : OI_CHG_NAME,
            u'成交量'   : VOLUME_NAME}, axis = 1, inplace = True)
        # replace the total row's key name
        df[PRODUCT_ID_NAME] = np.where(
            df[PRODUCT_GROUP_NAME].str.contains(u'小计', regex = True),
            TOTAL_ROW_KEY, df[PRODUCT_ID_NAME])
        df = normalize_history_data(df, u'总计')
        # clear the group name
        df[PRODUCT_GROUP_NAME] = df[PRODUCT_GROUP_NAME].str.replace(u'小计', '', regex = True)
        df = self.fix_product_id(df)
        df = calculate_index(df)
        return df

    #----------------------------------------------------------------------
    def fix_product_id(self, df: pd.DataFrame):
        """"""
        df[PRODUCT_ID_NAME] = df.apply(
            lambda row: row[PRODUCT_ID_NAME] if row[PRODUCT_ID_NAME] == TOTAL_ROW_KEY else self.instruments_map.get(row[PRODUCT_GROUP_NAME], '') + row[PRODUCT_ID_NAME],
            axis = 1)
        return df


#----------------------------------------------------------------------
class RemoteHttpDCEOptionsData(RemoteHttpDCEData):

    # http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html
    remote_path = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"
    request_post = True
    # for options
    trade_type = '1'

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        df = get_table_soup(data, GB_ENCODING, False)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename({
            u'合约名称' : PRODUCT_ID_NAME,
            u'商品名称' : PRODUCT_GROUP_NAME,
            u'前结算价' : PRE_SETTLE_PRICE_NAME,
            u'开盘价'   : OPEN_PRICE_NAME,
            u'最高价'   : HIGH_PRICE_NAME,
            u'最低价'   : LOW_PRICE_NAME,
            u'收盘价'   : CLOSE_PRICE_NAME,
            u'结算价'   : SETTLE_PRICE_NAME,
            u'持仓量'   : OPEN_INTEREST_NAME,
            u'持仓量变化' : OI_CHG_NAME,
            u'成交量'   : VOLUME_NAME}, axis = 1, inplace = True)
        # replace the total row's key name
        df[PRODUCT_ID_NAME] = np.where(
            df[PRODUCT_GROUP_NAME].str.contains(u'小计', regex = True),
            TOTAL_ROW_KEY, df[PRODUCT_ID_NAME])
        # clear the group name
        df[PRODUCT_GROUP_NAME] = df[PRODUCT_GROUP_NAME].str.replace(u'小计', '', regex = True)
        df = parse_options_name(df, OPTIONS_NAME_DASH_RE)
        df2 = self.get_underlying_close_price(df, date_str)
        df2 = normalize_options_data(df2)
        df2 = calculate_iv(df2)
        df2 = calculate_siv(df2)
        return df2


#----------------------------------------------------------------------
class RemoteHttpCZCEData(IRemoteHttpData):

    # http://www.czce.com.cn/cn/DFSStaticFiles/Future/2021/20210105/FutureDataDaily.htm
    remote_path = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataDaily.htm"
    proxy_mode = True

    #----------------------------------------------------------------------
    def get_remote_path(self, date: str):
        """query the remote data"""
        str_list = date.split('-')
        req_date = ''.join(str_list)
        return self.remote_path % (str_list[0], req_date)

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        df = get_table_soup(data, GB_ENCODING)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename({
            u'品种月份' : PRODUCT_ID_NAME,
            u'昨结算'  : PRE_SETTLE_PRICE_NAME,
            u'今开盘'  : OPEN_PRICE_NAME,
            u'最高价'  : HIGH_PRICE_NAME,
            u'最低价'  : LOW_PRICE_NAME,
            u'今收盘'  : CLOSE_PRICE_NAME,
            u'今结算'  : SETTLE_PRICE_NAME,
            u'持仓量'  : OPEN_INTEREST_NAME,
            # old name of open interest
            u'空盘量'  : OPEN_INTEREST_NAME,
            u'增减量'  : OI_CHG_NAME,
            u'成交量(手)': VOLUME_NAME}, axis = 1, inplace = True)
        # replace the total row's key name
        df = normalize_total_key(df, u'小计')
        # get the group name by product id
        df[PRODUCT_GROUP_NAME] = df[PRODUCT_ID_NAME].str.replace('\d+', '', regex = True)
        df[PRODUCT_GROUP_NAME] = np.where(df[PRODUCT_GROUP_NAME] == TOTAL_ROW_KEY,
                                          df[PRODUCT_GROUP_NAME].shift(1), df[PRODUCT_GROUP_NAME])
        df = normalize_history_data(df, u'总计')
        df = calculate_index(df)
        return df


#----------------------------------------------------------------------
class RemoteHttpCZCEOptionsData(RemoteHttpCZCEData):

    # http://www.czce.com.cn/cn/DFSStaticFiles/Option/2021/20210105/OptionDataDaily.htm
    remote_path = "http://www.czce.com.cn/cn/DFSStaticFiles/Option/%s/%s/OptionDataDaily.htm"
    proxy_mode = True

    #----------------------------------------------------------------------
    def do_data_handle(self, data, date_str: str):
        """"""
        df = get_table_soup(data, GB_ENCODING)
        df[INDEX_KEY] = date_str
        df.set_index(INDEX_KEY, inplace = True)
        df.rename({
            u'品种代码': PRODUCT_ID_NAME,
            u'昨结算': PRE_SETTLE_PRICE_NAME,
            u'今开盘': OPEN_PRICE_NAME,
            u'最高价': HIGH_PRICE_NAME,
            u'最低价': LOW_PRICE_NAME,
            u'今收盘': CLOSE_PRICE_NAME,
            u'今结算': SETTLE_PRICE_NAME,
            u'持仓量': OPEN_INTEREST_NAME,
            # old name of open interest
            u'空盘量'  : OPEN_INTEREST_NAME,
            u'增减量': OI_CHG_NAME,
            u'成交量(手)': VOLUME_NAME,
            u'隐含波动率': IV_NAME}, axis = 1, inplace = True)
        # replace the total row's key name
        df = normalize_total_key(df, u'小计')
        df = parse_options_name(df)
        # get the group name by product id
        df[PRODUCT_GROUP_NAME] = df[U_PRODUCT_ID_NAME].str.replace('\d+', '', regex = True)
        df[PRODUCT_GROUP_NAME] = np.where(df[PRODUCT_GROUP_NAME].isnull(),
                                          df[PRODUCT_ID_NAME],
                                          df[PRODUCT_GROUP_NAME])
        df[PRODUCT_GROUP_NAME] = np.where(df[PRODUCT_GROUP_NAME] == TOTAL_ROW_KEY,
                                          df[PRODUCT_GROUP_NAME].shift(1), df[PRODUCT_GROUP_NAME])
        df = df[~df[PRODUCT_ID_NAME].str.contains(u'合计', regex = True)]
        df2 = self.get_underlying_close_price(df, date_str)
        df2 = normalize_options_data(df2)
        df2 = calculate_iv(df2)
        df2 = calculate_siv(df2)
        return df2


#----------------------------------------------------------------------
class RemoteHttpExpiryDate(IRemoteHttpData):

    # http://www.shfe.com.cn/data/instrument/option/ContractBaseInfo20210105.dat
    remote_path = {'DCE': "http://www.shfe.com.cn/data/instrument/option/ContractBaseInfo%s.dat",
                   'm': "http://www.dce.com.cn/dalianshangpin/sspz/dpqq/index.html",
                   'c': "http://www.dce.com.cn/dalianshangpin/sspz/ymqq/index.html",
                   'i': "http://www.dce.com.cn/dalianshangpin/sspz/tksqq21/index.html",
                   'LPG': "http://www.dce.com.cn/dalianshangpin/sspz/yhsyqqq/index.html"}


#----------------------------------------------------------------------
class RemoteDataFactory(metaclass = Singleton):

    data_path = DATA_ROOT

    def __init__(self, data_path: str = DATA_ROOT):
        """Constructor"""
        self.data_path = data_path
        make_sure_dirs_exist(self.data_path)

    #----------------------------------------------------------------------
    def create(self, local: str, via: SYNC_DATA_MODE, dates: pd.Index, df_extra: pd.DataFrame):
        """the creator of RemoteData"""
        data_class = None
        if SYNC_DATA_MODE.HTTP_DOWNLOAD_CSINDEX_000300 == via:
            data_class = RemoteHttpCSIndex000300Data
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE == via:
            data_class = RemoteHttpCFFEData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_CALENDAR == via:
            data_class = RemoteHttpCFFETradingCalendar
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CFFE_OPTIONS == via:
            data_class = RemoteHttpCFFEOptionsData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_SHFE == via:
            data_class = RemoteHttpSHFEData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_SHFE_OPTIONS == via:
            data_class = RemoteHttpSHFEOptionsData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_DCE == via:
            data_class = RemoteHttpDCEData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_DCE_OPTIONS == via:
            data_class = RemoteHttpDCEOptionsData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE == via:
            data_class = RemoteHttpCZCEData
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CZCE_OPTIONS == via:
            data_class = RemoteHttpCZCEOptionsData
        if data_class != None:
            return data_class(self.data_path, local, dates, df_extra)
        raise NotImplementedError


remote_data_fac = RemoteDataFactory(DATA_ROOT)
