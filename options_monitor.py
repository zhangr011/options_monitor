# encoding: UTF-8

from options_monitor.utilities_calendar import get_last_trade_dates
from options_monitor.data_ref import SCHEDULE_HOUR
from options_monitor.utilities import mk_notification
from options_monitor.utilities_hv import sort_hv20250
from options_monitor.schedule_manager import ScheduleManager
from options_monitor.util_dingding import send_md_msg
from options_monitor.data_manager import \
    CSIndex000300DataManager, CFFEDataManager, SHFEDataManager, \
    DCEDataManager, CZCEDataManager
from options_monitor.logger import logger
from time import sleep
import threadpool


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = f'05 {SCHEDULE_HOUR} * * *'
    _day_index = None
    pool_size = 10
    _pushed_date = None

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        dates = get_last_trade_dates()
        csindex000300_mgr = CSIndex000300DataManager(dates)
        # cffe_mgr = CFFEDataManager(dates)
        shfe_mgr = SHFEDataManager(dates)
        dce_mgr = DCEDataManager(dates)
        czce_mgr = CZCEDataManager(dates)
        requests = threadpool.makeRequests(
            lambda x: x.download_raw_data(),
            # cffe_mgr, not used now
            [csindex000300_mgr, shfe_mgr, dce_mgr, czce_mgr])
        pool = threadpool.ThreadPool(self.pool_size)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        logger.info('all data downloaded. ')
        csindex300_dfs = csindex000300_mgr.analyze()
        shfe_dfs = shfe_mgr.analyze()
        dce_dfs = dce_mgr.analyze()
        czce_dfs = czce_mgr.analyze()
        all_dfs = csindex300_dfs + shfe_dfs + dce_dfs + czce_dfs
        this_date, final_df = sort_hv20250(all_dfs)
        if self._pushed_date != this_date:
            md_msg = mk_notification(final_df)
            send_md_msg(f'{this_date} hv', md_msg)
            self._pushed_date = this_date
        logger.info('schedule task done. ')
        return self.clear_and_return_true()

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        self._day_index = None
        return True


if __name__ == '__main__':
    mgr = MonitorScheduleManager(True)
    logger.info('options monitor started. ')
    while True:
        sleep(1)
