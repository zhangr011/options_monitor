# encoding: UTF-8
import unittest as ut
from black_scholes_cython import calculate_impv


#----------------------------------------------------------------------
class TestOptionPrice(ut.main):

    def testOptionPrice(self):
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
