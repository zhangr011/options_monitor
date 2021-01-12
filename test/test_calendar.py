# encoding: UTF-8

import unittest as ut

from options_monitor.data_manager import calendar_manager


#----------------------------------------------------------------------
class TestCFFECalendarManager(ut.TestCase):

    #----------------------------------------------------------------------
    def testCFFECalendarManager(self):
        calendar1 = calendar_manager.get_trading_calendar
        calendar2 = calendar_manager.get_trading_calendar
        self.assertEqual(calendar1.size, calendar2.size)
        self.assertEqual(True, calendar_manager.check_closed('2018-01-01'))
        self.assertEqual(False, calendar_manager.check_closed('2021-01-02'))
        self.assertEqual(False, calendar_manager.check_closed('2021-01-04'))


if __name__ == '__main__':
    ut.main()
