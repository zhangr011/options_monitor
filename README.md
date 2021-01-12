# options_monitor
国内期权监测。

从各大交易所获取期货和期权数据，计算期货历史波动率，计算期权综合隐含波动率。

## 数据获取方式

| 各交易所数据获取方式 | |
|---------------|--------------------------------------------------------------|
| **中证沪深300指数** | |
| Request URL | http://www.csindex.com.cn/zh-CN/indices/index-detail/000300?earnings_performance=1%E5%B9%B4&data_type=json |
| 参数 | earnings_performance 设置 3年/3个月 |
| 返回 | json 格式 |
| **中金所** | |
| Request URL | http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0 |
| 参数 | id 设置一个随机访问 id |
| 返回 | xml 格式 |
| **上期所** | |
| Request URL | http://www.shfe.com.cn/data/dailydata/kx/kx20210105.dat |
| Request URL | http://www.shfe.com.cn/data/dailydata/option/kx/kx20210105.dat |
| 返回 | json 格式 |
| **大商所** | |
| Request URL | http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html |
| Request Method | POST |
| Params | dayQuotes.variety：all<br />dayQuotes.trade_type: 0 （0 - 期货 1 - 期权）<br />year: 2021<br />month: 0 （不是从 1 开始）<br />day: 05 （前面补0） |
| 返回 | 网页，需要对网页解析 |
| **郑商所** | |
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Future/2021/20210105/FutureDataDaily.htm
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Option/2021/20210105/OptionDataDaily.htm
| 返回 | 网页，需要对网页进行解析 |

## 计算方法
### 商品指数
各品种商品指数以各月份持仓加权平均计算；
### 历史波动率 hv
v = sqrt（Σ（Xi - Xp）² / （n - 1））

Xi = ln（Pi / Pi-1），Pi 为第 i 天的收盘价；Xp 为 Xi 的平均值。
### 综合隐含波动率 siv
综合隐含波动率 = Σ（隐含波动率 × 成交量系数 × 距离系数）/ Σ（成交量系数 × 距离系数）

成交量系数 = 当前行权价成交量 / 所有行权价成交量

距离系数 = （行权价与标的物价格之间距离的百分比 - a）² / a² 当 x < a 时， a 是最大百分比距离，超过的权重为 0
