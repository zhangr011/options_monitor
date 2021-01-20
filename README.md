# options_monitor
国内期权监测。

从各大交易所获取期货和期权数据，计算期货历史波动率，计算期权综合隐含波动率。


## 使用方法
第一次下载数据耗时较长，且可能中途暂停，可能需要多次重新启动服务。
```bash
pip3 install -r ./requirements.txt
pip3 install .
# copy and edit your push.ini if needed.
cp ./data/push.back.ini ./data/push.ini
# start the monitor at back
bash ./start_monitor.sh restart &
# check if any error occurred
tail ./nohup.out
```

## 开发计划
### 已完成
#### 0.2.0 从交易所获取 k 线数据
#### 0.4.0 商品指数计算
#### 0.5.0 历史波动率计算
#### 0.6.0 历史波动率百分位计算
#### 0.7.0 根据 hv20 / hv250 的相对百分比排序，并通过 dingding 通知
### 计划中
#### 0.8.0 综合隐含波动率计算，综合隐含波动百分位计算

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
| Request URL | http://www.shfe.com.cn/data/instrument/option/ContractBaseInfo20210105.dat<br />按月获取，每个月的第一个交易日 |
| 返回 | json 格式 |
| **大商所** | |
| Request URL | http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html<br />POST<br />dayQuotes.variety：all<br />dayQuotes.trade_type: 0 （0 - 期货 1 - 期权）<br />year: 2021<br />month: 0 （不是从 1 开始）<br />day: 05 （前面补0） |
| Request URL | 豆粕 http://www.dce.com.cn/dalianshangpin/sspz/dpqq/index.html <br />玉米 http://www.dce.com.cn/dalianshangpin/sspz/ymqq/index.html <br />铁矿石 http://www.dce.com.cn/dalianshangpin/sspz/tksqq21/index.html <br />LPG http://www.dce.com.cn/dalianshangpin/sspz/yhsyqqq/index.html <br />塑料 http://www.dce.com.cn/dalianshangpin/sspz/6226615/index.html <br />pvc http://www.dce.com.cn/dalianshangpin/sspz/6226619/index.html <br />pp http://www.dce.com.cn/dalianshangpin/sspz/6226623/index.html |
| 返回 | 网页，需要对网页解析 |
| **郑商所** | |
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Future/2021/20210105/FutureDataDaily.htm
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Option/2021/20210105/OptionDataDaily.htm
| Request URL | http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp<br /> POST<br />tradetype: option<br />DtbeginDate: 2011-01-01<br />DtendDate: 2021-01-20<br />__go2pageNum: 1 |
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
