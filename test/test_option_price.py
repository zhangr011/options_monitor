# encoding: UTF-8

import unittest as ut
from black_scholes_cython import calculate_impv

from options_monitor.utilities import \
    TOTAL_ROW_KEY, IV_NAME, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    S_PRICE_NAME, U_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME
from options_monitor.remote_data import calculate_siv
import pandas as pd
import numpy as np
pd.set_option('mode.chained_assignment', None)


#----------------------------------------------------------------------
class TestOptionPrice(ut.TestCase):

    def testSIV(self):
        """"""
        df = pd.DataFrame(np.array([['01C30', 'i', 30, 33, 4.5, 50, 0.34],
                                    ['01C35', 'i', 35, 33, 1.5, 90, 0.28],
                                    ['04C35', 'i', 35, 33, 2.5, 55, 0.3],
                                    ['04C40', 'i', 40, 33, 1.5, 5, 0.38],
                                    [TOTAL_ROW_KEY, 'i', 0, 0, 0, 0, 0]]),
                          columns = [PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, S_PRICE_NAME, U_PRICE_NAME,
                                     CLOSE_PRICE_NAME, VOLUME_NAME, IV_NAME])
        df[S_PRICE_NAME] = df[S_PRICE_NAME].astype(float)
        df[U_PRICE_NAME] = df[U_PRICE_NAME].astype(float)
        df[VOLUME_NAME] = df[VOLUME_NAME].astype(int)
        df[IV_NAME] = df[IV_NAME].astype(float)
        df = calculate_siv(df)
        self.assertEqual(0.298, df.iloc[-1][IV_NAME])


    def _testOptionPrice(self):
        """"""
        iv = calculate_impv()
        # option_price 期权价格
        # underlying_price 标的物价格
        # strike_price 执行价
        # interest_rate 无风险利率
        # time_to_expiry 到期日
        # option_type 期权类型 1 call -1 put
        self.assertEqual(0, calculate_impv(option_price, underlying_price, strike_price, interest_rate, time_to_expiry, option_type))


if __name__ == '__main__':
    ut.main()
