# encoding: UTF-8

import unittest as ut
from options_monitor.data_ref import CLOSE_PRICE_NAME, IV_NAME
from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_manager import CZCEDataManager, CZCEOptionsDataManager
from options_monitor.remote_data import calculate_siv
import pandas as pd
pd.set_option('mode.chained_assignment', None)


#----------------------------------------------------------------------
class TestCZCEDataManager(ut.TestCase):

    def testCZCEDataManager(self):
        """"""
        dates = get_last_trade_dates()
        mgr = CZCEDataManager(dates)
        mgr.download_raw_data()
        _li, df_extra = mgr._remote_data.get_last_index()
        # self.assertEqual(False, (df[CLOSE_PRICE_NAME] == 0).any())
        mgr2 = CZCEOptionsDataManager(dates, df_extra)
        # download_raw_data to set _remote_data, so we need to do it before drop
        mgr2.download_raw_data()
        # mgr2._remote_data.drop_local_last_date_data()
        # mgr2.download_raw_data()
        _li, df = mgr2._remote_data.get_last_index()
        return
        df2 = calculate_siv(df.copy())
        mgr2._remote_data.save_data_test(df2)
        diff = df2[df2[IV_NAME] != df[IV_NAME]]
        self.assertEqual(True, diff.empty)


if __name__ == '__main__':
    ut.main()
