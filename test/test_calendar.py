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
        self.assertEqual(False, calendar_manager.check_open('2018-01-01'))
        self.assertEqual(False, calendar_manager.check_open('2020-10-08'))
        # special holiday for covid-19
        self.assertEqual(False, calendar_manager.check_open('2020-01-31'))
        self.assertEqual(False, calendar_manager.check_open('2021-01-01'))
        # not work for weekend by cffe calendar
        self.assertEqual(True, calendar_manager.check_open2('2021-01-02'))
        self.assertEqual(False, calendar_manager.check_open('2021-01-02'))
        self.assertEqual(True, calendar_manager.check_open('2021-01-04'))


if __name__ == '__main__':
    ut.main()
