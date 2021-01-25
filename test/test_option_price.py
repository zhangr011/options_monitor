# encoding: UTF-8

import unittest as ut

from options_monitor.data_ref import \
    INDEX_KEY, TOTAL_ROW_KEY, IV_NAME, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, \
    S_PRICE_NAME, U_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME
from options_monitor.remote_data import \
    calculate_iv, calculate_siv_by_volumes, calculate_siv_by_turnovers
from options_monitor.utilities_options import \
    calc_iv, fill_the_date, get_expiry_date, oc_mgr, calc_remained_days, \
    OPTIONS_TYPE_CALL, OPTIONS_TYPE_PUT
import pandas as pd
import numpy as np
pd.set_option('mode.chained_assignment', None)


#----------------------------------------------------------------------
class TestOptionPrice(ut.TestCase):

    #----------------------------------------------------------------------
    def testIV(self):
        current = '2021-01-25'
        iv = oc_mgr.calc_iv('IO2106-P-5600', 320, 5640, current)
        self.assertEqual(10, iv * 100)

    #----------------------------------------------------------------------
    def testSIV(self):
        """"""
        current = '2020-11-04'
        df = pd.DataFrame(np.array([[current, 'cu2103C30', 'cu', 30, 33, 4.5, 50, 0.34],
                                    [current, 'cu2103C35', 'cu', 35, 33, 1.5, 90, 0.28],
                                    [current, 'cu2105C35', 'cu', 35, 33, 2.5, 55, 0.3],
                                    [current, 'cu2105C40', 'cu', 40, 33, 1.5, 5, 0.38],
                                    [current, 'cu2105C30', 'cu', 45, 33, 3.5, 5, 0],
                                    [current, TOTAL_ROW_KEY, 'cu', 0, 0, 0, 0, 0]]),
                          columns = [INDEX_KEY, PRODUCT_ID_NAME, PRODUCT_GROUP_NAME, S_PRICE_NAME, U_PRICE_NAME,
                                     CLOSE_PRICE_NAME, VOLUME_NAME, IV_NAME])
        df.set_index(INDEX_KEY, inplace = True)
        df[S_PRICE_NAME] = df[S_PRICE_NAME].astype(float)
        df[U_PRICE_NAME] = df[U_PRICE_NAME].astype(float)
        df[CLOSE_PRICE_NAME] = df[CLOSE_PRICE_NAME].astype(float)
        df[VOLUME_NAME] = df[VOLUME_NAME].astype(int)
        df[IV_NAME] = df[IV_NAME].astype(float)
        df_iv = calculate_iv(df.copy())
        ivs = df_iv[IV_NAME].tolist()
        self.assertEqual([0.4109, 0.3242, 0.3803, 0.416, 0.1992, 0], ivs)
        # test for iv of 0
        df = calculate_siv_by_volumes(df)
        self.assertEqual(0.298, df.iloc[-1][IV_NAME])
        df = calculate_siv_by_turnovers(df)
        self.assertEqual(0.309, df.iloc[-1][IV_NAME])
        df2 = df[df[PRODUCT_ID_NAME] != 'cu2105C30']
        df2 = calculate_siv_by_volumes(df2)
        self.assertEqual(0.298, df2.iloc[-1][IV_NAME])
        df2 = calculate_siv_by_turnovers(df2)
        self.assertEqual(0.309, df2.iloc[-1][IV_NAME])


    #----------------------------------------------------------------------
    def testExpiryDate(self):
        """"""
        # for czce
        self.assertEqual('2019-01-01', fill_the_date('9', '01'))
        self.assertEqual('2018-05-01', fill_the_date('8', '05'))
        self.assertEqual('2020-09-01', fill_the_date('0', '09'))
        self.assertEqual('2021-12-01', fill_the_date('1', '12'))
        # for others
        self.assertEqual('2019-05-01', fill_the_date('19', '05'))
        self.assertEqual('2021-01-01', fill_the_date('21', '01'))
        self.assertEqual('2100-01-01', fill_the_date('00', '01', 365 * 80))
        self.assertEqual('2099-05-01', fill_the_date('99', '05', 365 * 80))
        # test io
        self.assertEqual('2021-06-18', get_expiry_date('IO', '2021-06-01'))
        self.assertEqual('2018-02-22', get_expiry_date('IO', '2018-02-01'))
        self.assertEqual('2021-06-18', oc_mgr.get_expiry_date_test('IO2106C5000'))
        self.assertEqual('2021-06-18', oc_mgr.get_expiry_date_test('IO2106-C-5000'))
        self.assertEqual('2018-02-22', oc_mgr.get_expiry_date_test('IO1802P3500'))
        self.assertEqual('2018-02-22', oc_mgr.get_expiry_date_test('IO1802-P-3500'))
        # test m, c, i, pg, l, v,
        self.assertEqual('2018-02-07', oc_mgr.get_expiry_date_test('m1803-C-2400'))
        self.assertEqual('2019-02-14', oc_mgr.get_expiry_date_test('m1903-C-2400'))
        self.assertEqual('2021-12-07', oc_mgr.get_expiry_date_test('m2201-P-3900'))
        self.assertEqual('2019-04-08', oc_mgr.get_expiry_date_test('c1905-C-1660'))
        self.assertEqual('2020-01-08', oc_mgr.get_expiry_date_test('i2002-C-580'))
        self.assertEqual('2020-10-15', oc_mgr.get_expiry_date_test('i2011-P-630'))
        self.assertEqual('2020-10-15', oc_mgr.get_expiry_date_test('pg2011-C-2100'))
        self.assertEqual('2020-12-07', oc_mgr.get_expiry_date_test('pg2101-C-2900'))
        self.assertEqual('2020-08-07', oc_mgr.get_expiry_date_test('l2009-C-6000'))
        self.assertEqual('2020-10-15', oc_mgr.get_expiry_date_test('l2011-P-5900'))
        self.assertEqual('2020-09-07', oc_mgr.get_expiry_date_test('v2010-P-7000'))
        self.assertEqual('2020-10-15', oc_mgr.get_expiry_date_test('v2011-C-7600'))
        self.assertEqual('2020-10-15', oc_mgr.get_expiry_date_test('pp2011-P-7600'))
        self.assertEqual('2020-12-07', oc_mgr.get_expiry_date_test('pp2101-P-7600'))
        self.assertEqual('2021-02-05', oc_mgr.get_expiry_date_test('pp2103-P-7600'))
        self.assertEqual('2021-04-08', oc_mgr.get_expiry_date_test('pp2105-P-7600'))
        # test CF, SR, RM, MA, TA, ZC
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('TA101C3300'))
        self.assertEqual('2019-04-03', oc_mgr.get_expiry_date_test('CF905C12000'))
        self.assertEqual('2019-06-05', oc_mgr.get_expiry_date_test('CF907P13000'))
        self.assertEqual('2020-10-13', oc_mgr.get_expiry_date_test('CF011C12200'))
        self.assertEqual('2017-11-24', oc_mgr.get_expiry_date_test('SR801C4500'))
        self.assertEqual('2018-01-25', oc_mgr.get_expiry_date_test('SR803P5000'))
        self.assertEqual('2019-05-27', oc_mgr.get_expiry_date_test('SR907C4800'))
        self.assertEqual('2019-08-05', oc_mgr.get_expiry_date_test('SR909P4800'))
        self.assertEqual('2020-05-08', oc_mgr.get_expiry_date_test('MA006C2000'))
        self.assertEqual('2020-05-08', oc_mgr.get_expiry_date_test('TA006C3500'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('CF101P13500'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('SR101C5000'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('RM101C2500'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('MA101P2200'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('TA101C3800'))
        self.assertEqual('2020-12-03', oc_mgr.get_expiry_date_test('ZC101P750'))
        # test cu, al, zn, au, ru
        # special holiday by covid-19, need to be skipped
        self.assertEqual('2020-01-20', oc_mgr.get_expiry_date_test('cu2002C48000'))
        self.assertEqual('2021-02-22', oc_mgr.get_expiry_date_test('cu2103C48000'))
        self.assertEqual('2021-03-25', oc_mgr.get_expiry_date_test('cu2104P44000'))
        self.assertEqual('2020-12-25', oc_mgr.get_expiry_date_test('cu2101C34000'))
        self.assertEqual('2021-01-25', oc_mgr.get_expiry_date_test('al2102C14400'))
        self.assertEqual('2021-04-26', oc_mgr.get_expiry_date_test('zn2105P23000'))
        self.assertEqual('2020-03-25', oc_mgr.get_expiry_date_test('au2004C316'))
        self.assertEqual('2020-07-27', oc_mgr.get_expiry_date_test('au2008P360'))
        self.assertEqual('2021-02-22', oc_mgr.get_expiry_date_test('ru2103C10000'))

    #----------------------------------------------------------------------
    def testCFFEOptionPrice(self):
        """"""
        current = '2021-01-22'
        days = calc_remained_days('IO', '2021-02-01', current)
        self.assertEqual(29, days)
        days = calc_remained_days('IO', '2021-06-01', current)
        self.assertEqual(148, days)
        ivc = oc_mgr.calc_iv('IO2106C5400', 350.6, 5569.78, current)
        self.assertAlmostEqual(19.17, ivc * 100, delta = 0.52) # too much
        ivp = oc_mgr.calc_iv('IO2106P5600', 356.6, 5579.78, current)
        self.assertAlmostEqual(25.02, ivp * 100, delta = 0.25)
        # 2021-01-22,IO2112-C-4400,IO,C,4400.0,5569.78,1155.0,4,46,0.0
        ivc = oc_mgr.calc_iv('IO2112-C-4400', 1155, 5569.78, '2021-01-22')
        # ivc = oc_mgr.calc_iv('IO2112-C-4400', 1172, 5569.78, '2021-01-22')
        self.assertEqual(0, ivc * 100)

    #----------------------------------------------------------------------
    def testSHFEOptionPrice(self):
        """"""
        current = '2021-01-22'
        days = calc_remained_days('cu', '2021-03-01', current)
        self.assertEqual(32, days)
        ivc = oc_mgr.calc_iv('cu2103C57000', 2220, 58810, current)
        self.assertAlmostEqual(16.27, ivc * 100, delta = 0.3)
        ivc = oc_mgr.calc_iv('cu2103C58000', 1550, 58810, current)
        self.assertAlmostEqual(16.35, ivc * 100, delta = 0.2)
        ivp = oc_mgr.calc_iv('cu2103P60000', 1896, 58810, current)
        self.assertAlmostEqual(17.70, ivp * 100, delta = 0.4) # too much
        ivp = oc_mgr.calc_iv('cu2103P59000', 1234, 58810, current)
        self.assertAlmostEqual(16.78, ivp * 100, delta = 0.55) # too much
        days = calc_remained_days('au', '2021-04-01', current)
        self.assertEqual(63, days)
        ivc = oc_mgr.calc_iv('au2104P396', 14.22, 390, current)
        self.assertAlmostEqual(17.81, ivc * 100, delta = 0.52) # too much
        ivp = oc_mgr.calc_iv('au2104C384', 14.8, 390, current)
        self.assertAlmostEqual(19.06, ivp * 100, delta = 0.61) # too much

    #----------------------------------------------------------------------
    def testDCEOptionPrice(self):
        """"""
        current = '2021-01-22'
        days = calc_remained_days('pp', '2021-05-01', current)
        self.assertEqual(77, days)
        ivc = oc_mgr.calc_iv('pp2105C7700', 618, 8170, current)
        self.assertAlmostEqual(24.04, ivc * 100, delta = 0.7) # too much
        ivp = oc_mgr.calc_iv('pp2105P8100', 272, 8170, current)
        self.assertAlmostEqual(21.63, ivp * 100, delta = 1.01)  # too much

    #----------------------------------------------------------------------
    def testCZCEOptionPrice(self):
        """"""
        current = '2021-01-22'
        self.assertEqual(13, calc_remained_days('SR', '2021-03-01', current))
        ivc = oc_mgr.calc_iv('SR103C5000', 224, 5216, current)
        self.assertAlmostEqual(18.56, ivc * 100, delta = 0.51) # too much
        ivp = oc_mgr.calc_iv('SR103P5300', 127.5, 5216, current)
        self.assertAlmostEqual(21.02, ivp * 100, delta = 1.4) # too too too much


    def testOptionPrice2(self):
        """"""
        # option_price 期权价格
        # underlying_price 标的物价格
        # strike_price 执行价
        # interest_rate 无风险利率
        # time_to_expiry 到期日
        # option_type 期权类型 1 call -1 put
        # calculate_impv(option_price, underlying_price, strike_price, interest_rate, time_to_expiry, option_type)
        # 2021-01-19,ZC103C810,ZC,C,810.0,770.8,6.4,41.22,1763,796
        ir = 0.0435
        gid = 'ZC'
        close = 770.8
        ivc = calc_iv(gid, 6.4, close, 810, 15, OPTIONS_TYPE_CALL)
        # 2021-01-19,ZC103P810,ZC,P,810.0,770.8,55.0,41.22,261,134
        ivp = calc_iv(gid, 55, close, 810, 15, OPTIONS_TYPE_PUT)
        total = 1763 + 261
        self.assertAlmostEqual(41.22, (ivc + ivp) / 2 * 100, delta = 0.8) # too much
        ivc2 = calc_iv(gid, 0.2, close, 780, 2, OPTIONS_TYPE_CALL)
        ivc = calc_iv(gid, 0.2, close, 780, 1, OPTIONS_TYPE_CALL)
        # test for days left 0
        self.assertEqual(10.97, ivc2 * 100)
        self.assertEqual(15.52, ivc * 100)
        # 2021-01-19,ZC109C650,ZC,C,650.0,639.6,59.5,28.65,1,1
        ivc = calc_iv(gid, 59.5, 639.6, 650, 197, OPTIONS_TYPE_CALL)
        # 2021-01-19,ZC109P650,ZC,P,650.0,639.6,48.5,28.65,11,7
        ivp = calc_iv(gid, 48.5, 639.6, 650, 197, OPTIONS_TYPE_PUT)
        self.assertAlmostEqual(28.65, (ivc + ivp) / 2 * 100, delta = 1) # too much


if __name__ == '__main__':
    ut.main()
