# encoding: UTF-8

import unittest as ut

from options_monitor.utilities import get_last_year_trade_dates
from options_monitor.data_manager import CSIndex000300DataManager


#----------------------------------------------------------------------
class TestCSIndex000300Data(ut.TestCase):

    def testData(self):
        dates = get_last_year_trade_dates()
        mgr = CSIndex000300DataManager(dates)
