# encoding: UTF-8

import unittest as ut

from options_monitor.utilities import get_last_year_trade_dates


#----------------------------------------------------------------------
class TestUtilities(ut.TestCase):

    def testGetTradeDates(self):
        """"""
        dates_df = get_last_year_trade_dates(30)
        self.assertListEqual(False, dates_df.empty)


if __name__ == '__main__':
    ut.main()
