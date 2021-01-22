# encoding: UTF-8

import unittest as ut
from options_monitor.data_ref import CLOSE_PRICE_NAME
from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_manager import CZCEDataManager, CZCEOptionsDataManager
import pandas as pd
pd.set_option('mode.chained_assignment', None)


#----------------------------------------------------------------------
class TestCZCEDataManager(ut.TestCase):

    def testCZCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = CZCEDataManager(dates)
        mgr.download_raw_data()
        _li, df = mgr._remote_data.get_last_index()
        # self.assertEqual(False, (df[CLOSE_PRICE_NAME] == 0).any())
        mgr2 = CZCEOptionsDataManager(dates, df)
        mgr2.download_raw_data()


if __name__ == '__main__':
    ut.main()
