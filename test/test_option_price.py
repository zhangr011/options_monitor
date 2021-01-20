# encoding: UTF-8

import unittest as ut

try:
    import black_76_cython as black_76
    import binomial_tree_cython as binomial_tree
    import black_scholes_cython as black_scholes
except ImportError:
    from .pricing import (
        black_76, binomial_tree, black_scholes
    )
    logger.info("Faile to import cython option pricing model, please rebuild with cython in cmd.")
    logger.info('pip3 install ./pricing/cython_model/binomial_tree_cython')
    logger.info('pip3 install ./pricing/cython_model/black_76_cython')
    logger.info('pip3 install ./pricing/cython_model/black_scholes_cython')

from options_monitor.utilities import \
    TOTAL_ROW_KEY, IV_NAME, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    S_PRICE_NAME, U_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME
from options_monitor.remote_data import calculate_siv
from options_monitor.utilties_option import calc_iv, PriceOptionType
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


    def testOptionPrice(self):
        """"""
        # option_price 期权价格
        # underlying_price 标的物价格
        # strike_price 执行价
        # interest_rate 无风险利率
        # time_to_expiry 到期日
        # option_type 期权类型 1 call -1 put
        # calculate_impv(option_price, underlying_price, strike_price, interest_rate, time_to_expiry, option_type)
        # 2021-01-19,ZC103C810,ZC,C,810.0,770.8,6.4,41.22,1763,796
        years = 365
        ir = 0.0435
        gid = 'ZC'
        close = 770.8
        ivc = calc_iv(gid, 6.4, close, 810, 15, PriceOptionType.CALL)
        # 2021-01-19,ZC103P810,ZC,P,810.0,770.8,55.0,41.22,261,134
        ivp = calc_iv(gid, 55, close, 810, 15, PriceOptionType.PUT)
        total = 1763 + 261
        print(ivc, ivp)
        self.assertEqual(41.22, (ivc + ivp) / 2 * 100)
        # 2021-01-19,ZC109C650,ZC,C,650.0,639.6,59.5,28.65,1,1
        ivc = calc_iv(gid, 59.5, 639.6, 650, 197, PriceOptionType.CALL)
        # 2021-01-19,ZC109P650,ZC,P,650.0,639.6,48.5,28.65,11,7
        ivp = calc_iv(gid, 48.5, 639.6, 650, 197, PriceOptionType.PUT)
        print(ivc, ivp)
        self.assertEqual(28.65, (ivc + ivp) / 2 * 100)


if __name__ == '__main__':
    ut.main()
