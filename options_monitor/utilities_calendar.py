# encoding: UTF-8

import pandas as pd
import datetime
from .data_ref import DATE_FORMAT, sse_calendar, SCHEDULE_HOUR
from .data_manager import calendar_manager


ONE_DAY = datetime.timedelta(days = 1)
SEVEN_DAYS = datetime.timedelta(days = 7)


#----------------------------------------------------------------------
def get_last_trade_dates(delta: int = 500):
    delta = int(delta)
    end_time = datetime.datetime.now(sse_calendar.tz)
    utc_end_time = end_time.astimezone(datetime.timezone.utc)
    if utc_end_time.hour < SCHEDULE_HOUR or utc_end_time.day != end_time.day:
        # before the schedule timestamp, back 1 day
        end_time += datetime.timedelta(days = -1)
    # about 13 months ago
    start_time = end_time + datetime.timedelta(days = -delta)
    dates = pd.date_range(start = start_time, end = end_time, freq = 'B')
    dates = dates.strftime(DATE_FORMAT)
    fdates = dates[dates.map(calendar_manager.check_open)]
    return fdates


#----------------------------------------------------------------------
def get_next_trading_day_str(time: pd.Timestamp, shift_days: int = 1):
    """"""
    idx = 0
    shift_times = abs(shift_days)
    shift_delta = shift_days // shift_times
    while True:
        day_str = time.strftime(DATE_FORMAT)
        if calendar_manager.check_open(day_str, False):
            idx += 1
        if idx >= shift_times:
            return day_str
        if shift_delta > 0 and time.day_of_week == 4:
            # friday to monday directly
            delta = 3
        elif shift_delta < 0 and time.day_of_week == 0:
            # monday to friday directly
            delta = -3
        else:
            delta = 1 * shift_delta
        time += datetime.timedelta(days = delta)
