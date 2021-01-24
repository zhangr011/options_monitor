# encoding: UTF-8

from enum import Enum
import re, os
import pandas as pd
import pandas_market_calendars as market_cal


# Date format: Year-Month-Day
DATE_FORMAT ='%Y-%m-%d'
DATE_FORMAT_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')

# data root
DATA_ROOT = './data'
TEST_DATA_ROOT = './test/data'

# shanghai stock exchange
sse_calendar = market_cal.get_calendar('SSE')
# timezone('Asia/Shanghai')
TZ_INFO = sse_calendar.tz.zone

SCHEDULE_HOUR = 11

#----------------------------------------------------------------------
class SYNC_DATA_MODE(Enum):
    HTTP_DOWNLOAD_CFFE          = 11
    HTTP_DOWNLOAD_CFFE_OPTIONS  = 12
    # trading calendar
    HTTP_DOWNLOAD_CFFE_CALENDAR = 15
    HTTP_DOWNLOAD_SHFE          = 21
    HTTP_DOWNLOAD_SHFE_OPTIONS  = 22
    HTTP_DOWNLOAD_DCE           = 31
    HTTP_DOWNLOAD_DCE_OPTIONS   = 32
    HTTP_DOWNLOAD_CZCE          = 41
    HTTP_DOWNLOAD_CZCE_OPTIONS  = 42
    # 沪深 300 指数
    HTTP_DOWNLOAD_CSINDEX_000300 = 51


# index key for file check
INDEX_KEY = 'Trade Date'
PRODUCT_ID_NAME = 'ProductId'
PRODUCT_GROUP_NAME = 'GroupId'
PRE_SETTLE_PRICE_NAME = 'PreSettle'
SETTLE_PRICE_NAME = 'Settle'
OPEN_PRICE_NAME = 'Open'
HIGH_PRICE_NAME = 'High'
LOW_PRICE_NAME = 'Low'
CLOSE_PRICE_NAME = 'Close'
OPEN_INTEREST_NAME = 'OI'
OI_CHG_NAME = 'OIChange'
VOLUME_NAME = 'Volume'

# for options
IV_NAME = 'iv'
U_PRODUCT_ID_NAME = 'u_id' # 标的物
OPTION_TYPE_NAME = 'otype' # 期权类型 c call p put
S_PRICE_NAME = 's_price'   # 行权价
U_PRICE_NAME = 'u_price'   # 标的价
EXPIRY_NAME = 'expiry'     # 到期日
TURNOVER_NAME = 'turnover' # 成交金额


# hv key
HV_20_NAME = 'hvm'
HV_250_NAME = 'hvy'
HV_20_250_NAME = 'hvm/y'
HV_PER = 'hmp'
HV_MIN = 'hml'
HV_MAX = 'hmh'
IV_PER = 'ivp'
IV_MIN = 'ivl'
IV_MAX = 'ivh'

# total key for future's index
TOTAL_ROW_KEY = 'total'

COLUMN_NAMES = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, PRE_SETTLE_PRICE_NAME,
                OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, SETTLE_PRICE_NAME,
                OPEN_INTEREST_NAME, OI_CHG_NAME, VOLUME_NAME]

O_COLUMN_NAMES = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, OPTION_TYPE_NAME, S_PRICE_NAME,
                  U_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME, OPEN_INTEREST_NAME]


# futures names for hv notification
FUTURE_HV_NAMES = {
    # cs
    'csidx300' : '300',
    # shfe
    'cu_f' : u'沪铜',
    'al_f' : u'沪铝',
    'zn_f' : u'沪锌',
    'pb_f' : u'沪铅',
    'ni_f' : u'沪镍',
    'sn_f' : u'沪锡',
    'au_f' : u'沪金',
    'ag_f' : u'沪银',
    'rb_f' : u'螺纹',
    'hc_f' : u'热卷',
    'ss_f' : u'ss',
    'sc_f' : u'原油',
    'fu_f' : u'燃油',
    'lu_f' : u'LU',
    'bu_f' : u'沥青',
    'ru_f' : u'橡胶',
    'nr_f' : u'20胶',
    'sp_f' : u'纸浆',
    # DCE
    u'豆一' : u'豆一',
    u'玉米' : u'玉米',
    u'苯乙烯' : u'EB',
    u'乙二醇' : u'eg',
    u'铁矿石' : u'铁',
    u'焦炭' : u'焦炭',
    u'鸡蛋' : u'鸡蛋',
    u'焦煤' : u'焦煤',
    u'聚乙烯' : u'塑料',
    u'生猪' : u'生猪',
    u'豆粕' : u'豆粕',
    u'棕榈油' : u'棕榈',
    u'液化石油气' : u'LPG',
    u'聚丙烯' : u'pp',
    u'粳米' : u'粳米',
    u'聚氯乙烯' : u'pvc',
    u'豆油' : u'豆油',
    # CZCE
    'AP' : u'苹果',
    'CF' : u'郑棉',
    'CJ' : u'红枣',
    # 'CY' : u'棉纱',
    'FG' : u'玻璃',
    # 'LR' : u'晚稻',
    'MA' : u'郑醇',
    'OI' : u'郑油',
    # 'PM' : u'普麦',
    # 'RI' : u'早稻',
    'RM' : u'菜粕',
    # 'RS' : u'菜籽',
    'SA' : u'纯碱',
    'SF' : u'硅铁',
    'SM' : u'锰硅',
    'SR' : u'白糖',
    'TA' : u'TA',
    'UR' : u'尿素',
    # 'WH' : u'郑麦',
    'ZC' : u'动煤'
}


#----------------------------------------------------------------------
def check_date_in(date_str: str, dates: pd.Index):
    return date_str in dates


#----------------------------------------------------------------------
def make_sure_dirs_exist(path):
    """确保目录存在"""
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)
    if not os.path.exists(path):
        return False
    return True
