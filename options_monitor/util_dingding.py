# encoding: UTF-8

import logging, configparser, os
import time, hmac, hashlib, base64, urllib.parse
import requests, threading, traceback, json
import pandas as pd

from .utilities import DATA_ROOT
from .data_ref import IV_PER, IV_NAME, IV_MIN, IV_MAX
from .logger import logger


ini_config = configparser.ConfigParser()
PUSH_CONFIG_PATH = os.path.join(DATA_ROOT, 'push.ini')
PUSH_SECTION = 'ddpusher'
ini_config.read(PUSH_CONFIG_PATH)

# dingding push
DD_URL = ini_config.get(PUSH_SECTION, 'url')
DD_TOKEN = ini_config.get(PUSH_SECTION, 'token')
HTTP_URL = ini_config.get(PUSH_SECTION, 'html_url')
FLASK_URL = ini_config.get(PUSH_SECTION, 'html_url_flask')
FILE_PATH = ini_config.get(PUSH_SECTION, 'file_path')
DD_TOKEN_ENC = DD_TOKEN.encode('utf-8')

headers = {'Content-Type': 'application/json'}


#----------------------------------------------------------------------
def generate_timestamp():
    """生成时间戳"""
    return str(round(time.time() * 1000))


#----------------------------------------------------------------------
def generate_sign(timestamp: str):
    """生成签名"""
    string_to_sign = f'{timestamp}\n{DD_TOKEN}'
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(DD_TOKEN_ENC, string_to_sign_enc,
                         digestmod = hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return sign


#----------------------------------------------------------------------
def do_send_msg(msg: str):
    """发送消息"""
    timestamp = generate_timestamp()
    sign = generate_sign(timestamp)
    try:
        response = requests.post(
            DD_URL + f'&timestamp={timestamp}&sign={sign}',
            headers = headers, data = json.dumps(msg))
        res = json.loads(response.content)
        if int(res.get('errcode', 1)) != 0:
            logger.error(res)
        else:
            logger.info(res)
    except requests.exceptions.ConnectionError:
        logger.error(traceback.format_exc(limit = 1))


#----------------------------------------------------------------------
def send_msg(msg: str):
    """在线程中发送消息"""
    thread = threading.Thread(target = do_send_msg, args = (msg, ))
    thread.start()


#----------------------------------------------------------------------
def send_md_msg(title: str, content: str):
    """发送 md 信息"""
    content = f'### {title}  \n\n  --------------  \n\n  {content}  \n\n'
    msg = {'msgtype': "markdown",
           'markdown': {"title": title,
                        "text": content}}
    send_msg(msg)


#----------------------------------------------------------------------
def get_http_params(date_str: str):
    """"""
    filename = date_str + '.html'
    link = os.path.join(HTTP_URL, filename)
    flask_link = os.path.join(FLASK_URL, f'300/{date_str}')
    local_path = os.path.join(FILE_PATH, filename)
    return link, flask_link, local_path


#----------------------------------------------------------------------
def send_html_msg(date_str: str, df: pd.DataFrame, send: bool = True):
    """将 dataframe 存为 html 之后发送带 html 的链接"""
    link, flask_link, local_path = get_http_params(date_str)
    df.reset_index(inplace = True)
    # ivp check
    df_warn = df[df[IV_PER] != '-']
    df_warn = df_warn[(df_warn[IV_PER].astype(int) >= 95) | (df_warn[IV_PER].astype(int) <= 15)]
    # iv check
    df_warn2 = df[(df[IV_PER] == '-') & (df[IV_NAME] != '-')]
    df_warn2 = df_warn2[(df_warn2[IV_NAME].astype(float) >= df_warn2[IV_MAX].astype(float) * 0.9) |
                        (df_warn2[IV_NAME].astype(float) <= df_warn2[IV_MIN].astype(float) * 1.1)]
    df_warn = df_warn.append(df_warn2)
    warn_msg = ''
    for index, row in df_warn.iterrows():
        warn_msg += f"> {row['name']}: iv {row[IV_NAME]} p {row[IV_PER]}  \n  "
    df.to_html(buf = local_path, bold_rows = False, classes = 'table table-striped', encoding = 'utf_8_sig')
    title = f"daily report: {date_str}"
    msg = {'msgtype': "markdown",
           'markdown': {"title": title,
                        "text": f"#### {title} \n {warn_msg}> [for details...]({link})  \n  > [svix viewer...]({flask_link}) \n"}}
    if send is True:
        send_msg(msg)
