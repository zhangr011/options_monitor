# encoding: UTF-8

import unittest as ut
import pandas as pd

from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_manager import \
    CFFEDataManager, CFFEOptionsDataManager, CSIndex000300DataManager
pd.set_option('mode.chained_assignment', None)


#----------------------------------------------------------------------
class TestCFFEData(ut.TestCase):

    def testData(self):
        dates = get_last_trade_dates()
        mgr = CFFEDataManager(dates)
        mgr.download_raw_data()
        mgr_index = CSIndex000300DataManager(dates)
        mgr_index.download_raw_data()
        _li, df_extra = mgr_index._remote_data.get_last_index()
        mgr2 = CFFEOptionsDataManager(dates, df_extra)
        mgr2.download_raw_data()


if __name__ == '__main__':
    ut.main()
