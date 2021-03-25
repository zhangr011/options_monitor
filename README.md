# options_monitor
国内期权监测。

从各大交易所获取期货和期权数据，计算期货历史波动率，计算期权综合隐含波动率。


## 使用方法
由于郑商所使用了反爬，所以需要先安装 proxy_pool
```bash
cd 3rd/proxy_pool.git
sudo apt-get install libxml2-dev libxslt1-dev
sudo pip install lxml
pip3 install -r ./requirements.txt
pip3 install werkzeug --upgrade
```
更新配置
```python
# setting.py 为项目配置文件

# 配置API服务

HOST = "0.0.0.0"               # IP
PORT = 5000                    # 监听端口


# 配置数据库

DB_CONN = 'redis://:pwd@127.0.0.1:8888/0'


# 配置 ProxyFetcher

PROXY_FETCHER = [
    "freeProxy01",      # 这里是启用的代理抓取方法名，所有fetch方法位于fetcher/proxyFetcher.py
    "freeProxy02",
    # ....
]
```
启动 proxy_pool
```bash
# 如果已经具备运行条件, 可用通过proxyPool.py启动。
# 程序分为: schedule 调度程序 和 server Api服务

# 启动调度程序，启动webApi服务
bash ./start_proxy_pool.sh -m restart
```

第一次下载数据耗时较长，且可能中途暂停，可能需要多次重新启动服务。

```bash
# 期权定价模型 cython 版本编译
pip3 install Cython
cd ./pricing/cython_model/binomial_tree_cython/
pip3 install .
cd ./pricing/cython_model/black_76_cython/
pip3 install .
cd ./pricing/cython_model/black_scholes_cython/
pip3 install .
```
```bash
pip3 install -r ./requirements.txt
pip3 install .
# copy and edit your push.ini if needed.
cp ./data/push.back.ini ./data/push.ini
# start the monitor at back
# -m start|restart|stop
# -i ananlyze immediately
# -p push message by dingding
# -r recalculate siv, this maybe called when siv calculation method changed
bash ./start_monitor.sh -m restart -p 2>1& &
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
#### 0.8.0 综合隐含波动率计算，综合隐含波动百分位计算
#### 0.9.0 增加反爬虫手段
#### 0.10.0 增加 siv web 浏览页面，使用 flask 和 pyecharts （最终转移至 https://github.com/zhangr011/vix_web_viewer.git）
### 计划中

## 数据获取方式

| 各交易所数据获取方式 | |
|---------------|--------------------------------------------------------------|
| **中证沪深300指数** | |
| Request URL | http://www.csindex.com.cn/zh-CN/indices/index-detail/000300?earnings_performance=1%E5%B9%B4&data_type=json<br />"http://www.csindex.com.cn/zh-CN/homeApply" 收盘价更新更及时 |
| 参数 | earnings_performance 设置 3年/3个月 |
| 返回 | json 格式 |
| **中金所** | |
| Request URL | http://www.cffex.com.cn/sj/hqsj/rtj/202101/05/index.xml?id=0<br />沪深 300 合约到期月份的第三个星期五，遇国家法定假日顺延 |
| 参数 | id 设置一个随机访问 id |
| 返回 | xml 格式 |
| **上期所** | |
| Request URL | http://www.shfe.com.cn/data/dailydata/kx/kx20210105.dat |
| Request URL | http://www.shfe.com.cn/data/dailydata/option/kx/kx20210105.dat |
| Request URL | http://www.shfe.com.cn/data/instrument/option/ContractBaseInfo20210105.dat<br />标的期货合约到期月份前一个月的倒数第 5 个交易日 |
| 返回 | json 格式 |
| **大商所** | |
| Request URL | http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html<br />POST<br />dayQuotes.variety：all<br />dayQuotes.trade_type: 0 （0 - 期货 1 - 期权）<br />year: 2021<br />month: 0 （不是从 1 开始）<br />day: 05 （前面补0） |
| Request URL | 豆粕 http://www.dce.com.cn/dalianshangpin/sspz/dpqq/index.html <br />玉米 http://www.dce.com.cn/dalianshangpin/sspz/ymqq/index.html <br />铁矿石 http://www.dce.com.cn/dalianshangpin/sspz/tksqq21/index.html <br />LPG http://www.dce.com.cn/dalianshangpin/sspz/yhsyqqq/index.html <br />塑料 http://www.dce.com.cn/dalianshangpin/sspz/6226615/index.html <br />pvc http://www.dce.com.cn/dalianshangpin/sspz/6226619/index.html <br />pp http://www.dce.com.cn/dalianshangpin/sspz/6226623/index.html <br />标的期货合约到期月份前一个月第 5 个交易日 |
| 返回 | 网页，需要对网页解析 |
| **郑商所** | |
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Future/2021/20210105/FutureDataDaily.htm
| Request URL | http://www.czce.com.cn/cn/DFSStaticFiles/Option/2021/20210105/OptionDataDaily.htm
| Request URL | http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp<br /> POST<br />tradetype: option<br />DtbeginDate: 2011-01-01<br />DtendDate: 2021-01-20<br />__go2pageNum: 1 <br />标的期货合约到期月份前一个月第 3 个交易日 |
| 返回 | 网页，需要对网页进行解析 |

## 计算方法
### 商品指数
各品种商品指数以各月份持仓加权平均计算；
### 历史波动率 hv
v = sqrt（Σ（Xi - Xp）² / （n - 1））

Xi = ln（Pi / Pi-1），Pi 为第 i 天的收盘价；Xp 为 Xi 的平均值。
### 综合隐含波动率 siv
综合隐含波动率 = Σ（隐含波动率 × 成交量 × 距离系数）/ Σ（成交量 × 距离系数）

距离系数 = （行权价与标的物价格之间距离的百分比 - a）² / a² 当 x < a 时， a 是最大百分比距离，超过的权重为 0

上述计算方式，当近月期权即将到期时，会有大量的近月低价值期权干扰（由于距离交割日比较近，计算出的隐含波动率往往不准确，参考意义不大）
因此在实际使用时，采用成交额系数替换成交量系数

综合隐含波动率 = Σ（隐含波动率 × 成交额 × 距离系数）/ Σ（成交额 × 距离系数）
其中成交额 = 期权收盘价 × 成交量
