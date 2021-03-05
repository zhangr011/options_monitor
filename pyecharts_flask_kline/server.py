from random import randrange

from flask import Flask, render_template

from typing import List, Sequence, Union
from functools import lru_cache
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pyecharts.charts import Kline, Line, Bar, Grid, Tab
from pyecharts.globals import ThemeType

from options_monitor.data_manager import SIVManager
from options_monitor.data_ref import \
    PRODUCT_GROUP_NAME, FUTURE_HV_NAMES_REVERSE, \
    IV_NAME, OPEN_INTEREST_NAME, HV_20_NAME, HV_250_NAME, CLOSE_PRICE_NAME, VOLUME_NAME

import pandas as pd


app = Flask(__name__, static_folder="templates")


THEME_ME = ThemeType.WHITE


#----------------------------------------------------------------------
@lru_cache
def get_siv_info(now_date_str: str):
    """analyze"""
    siv_mgr = SIVManager()
    all_dfs = siv_mgr.prepare(None, now_date_str)
    return all_dfs


#----------------------------------------------------------------------
def get_iv_data(product: str, date_str: str):
    """get the iv data by contract and date"""
    analyze_dfs = get_siv_info(date_str)
    product_rev = FUTURE_HV_NAMES_REVERSE.get(product)
    for df in analyze_dfs:
        if df[PRODUCT_GROUP_NAME][0] == product_rev:
            return df[df.index <= date_str]


#----------------------------------------------------------------------
def kline_chart(data: pd.DataFrame, product: str):
    # 最后的 Grid
    grid_chart = Grid(init_opts = opts.InitOpts(theme = THEME_ME))
    dates = data.index.to_list()

    kline = (
        Kline(init_opts = opts.InitOpts())
        .add_xaxis(xaxis_data = dates)
        .add_yaxis(
            series_name = "kline",
            y_axis = list(zip(data[CLOSE_PRICE_NAME], data[CLOSE_PRICE_NAME],
                              data[CLOSE_PRICE_NAME] * 0.995, data[CLOSE_PRICE_NAME] * 1.005)),
            yaxis_index = 0,
            itemstyle_opts=opts.ItemStyleOpts(
                color="#ef232a",
                color0="#14b143",
                border_color="#ef232a",
                border_color0="#14b143",
            ),
        )
        .set_series_opts(
            label_opts = opts.LabelOpts(is_show=False),
            # 'circle', 'rect', 'roundRect', 'triangle', 'diamond', 'pin', 'arrow', 'none'
            markline_opts = opts.MarkLineOpts(
                data = [
                    opts.MarkLineItem(type_ = "min", name = "最低价", symbol = 'none'),
                    opts.MarkLineItem(type_ = "max", name = "最高价", symbol = 'none'),
                ]
            ),
        )
        .set_global_opts(
            title_opts = opts.TitleOpts(title = f"{product} siv", pos_left="0"),
            xaxis_opts = opts.AxisOpts(
                type_="category",
                is_scale=True,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts = opts.AxisOpts(
                name = "指数价格",
                position = "right",
                is_scale = True,
                splitline_opts = opts.SplitLineOpts(is_show = True),
            ),
            tooltip_opts = opts.TooltipOpts(trigger = "axis", axis_pointer_type = "line"),
            legend_opts = opts.LegendOpts(is_show = False),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=False, type_="inside", xaxis_index=[0, 0], range_end=100
                ),
                opts.DataZoomOpts(
                    is_show=True, xaxis_index=[0, 1], pos_top="97%", range_end=100
                ),
                opts.DataZoomOpts(is_show=False, xaxis_index=[0, 2], range_end=100),
            ],
        )
    )

    siv_line = (
        Line(init_opts = opts.InitOpts())
        .add_xaxis(xaxis_data = dates)
        .add_yaxis(
            series_name = "siv",
            y_axis = data[IV_NAME] * 100,
            yaxis_index = 1,
            # is_smooth=True,
            markline_opts = opts.MarkLineOpts(
                data = [
                    opts.MarkLineItem(type_ = "min", name = "ivl"),
                    opts.MarkLineItem(type_ = "max", name = "ivh"),
                ]
            ),
            linestyle_opts = opts.LineStyleOpts(opacity = 1, width = 2),
            label_opts = opts.LabelOpts(is_show = False),
        )
        .add_yaxis(
            series_name = "hv20",
            y_axis = data[HV_20_NAME] * 100,
            yaxis_index = 2,
            # is_smooth=True,
            linestyle_opts = opts.LineStyleOpts(opacity = 0.9, width = 1.2),
            label_opts = opts.LabelOpts(is_show = False),
        )
        .add_yaxis(
            series_name = "hv250",
            y_axis = data[HV_250_NAME] * 100,
            yaxis_index = 3,
            # is_smooth=True,
            linestyle_opts = opts.LineStyleOpts(opacity = 0.8, width = 1),
            label_opts = opts.LabelOpts(is_show = False),
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                grid_index=1,
                axislabel_opts = opts.LabelOpts(is_show = False),
            ),
            yaxis_opts=opts.AxisOpts(
                grid_index = 1,
                split_number = 4,
                # position = "right",
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
                axislabel_opts=opts.LabelOpts(is_show=True),
                splitline_opts = opts.SplitLineOpts(is_show = False),
            ),
        )
    )

    # options' volume / futures' open interest
    oi = []
    try:
        oi = data[OPEN_INTEREST_NAME]
    except KeyError:
        pass
    bar_1 = (
        Bar(init_opts = opts.InitOpts())
        .add_xaxis(xaxis_data = data.index)
        .add_yaxis(
            series_name = "futures oi",
            y_axis = oi,
            xaxis_index = 1,
            yaxis_index = 1,
            label_opts = opts.LabelOpts(is_show = False),
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                grid_index=2,
                axislabel_opts=opts.LabelOpts(is_show=False),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )

    # K线图和 MA5 的折线图
    pos_left_str = "3%"
    pos_right_str = "5%"
    grid_chart.add(
        kline,
        grid_opts = opts.GridOpts(pos_left = pos_left_str, pos_right = pos_right_str, height = "70%"),
    ).add(
        siv_line,
        grid_opts = opts.GridOpts(pos_left = pos_left_str, pos_right = pos_right_str, height = "70%"),
    ).add(
        bar_1,
        grid_opts = opts.GridOpts(
            pos_left = pos_left_str, pos_right = pos_right_str, pos_bottom = "3%", height = "15%"
        ),
    )
    return grid_chart

@app.route("/<product>/<date_str>")
def index(product: str, date_str: str):
    kwargs = {"product" : product,
              "date" : date_str,
              "tabs": FUTURE_HV_NAMES_REVERSE.keys()}
    return render_template('index.html', **kwargs)

@app.route("/siv/<product>/<date_str>")
def get_bar_chart(product: str, date_str: str):
    """kline data"""
    data = get_iv_data(product, date_str)
    kline = kline_chart(data, product)
    return kline.dump_options_with_quotes()


if __name__ == "__main__":
    app.run(host='0.0.0.0')
