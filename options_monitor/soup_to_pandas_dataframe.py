# encoding: UTF-8

import pandas as pd


# According to: https://stackoverflow.com/questions/23377533/python-beautifulsoup-parsing-table
#----------------------------------------------------------------------
def soup_to_pandas_dataframe(soup, thead_tbody: bool = True):
    tables = soup.find_all("table")
    tab = tables[0]
    titles = []
    if thead_tbody:
        head = tab.find('thead')
        title_list = head.find_all('td')
        body = tab.find('tbody')
    else:
        title_list = tab.find_all('th')
        body = tab
    for title in title_list:
        titles.append(title.text.strip())
    data = []
    for row in body.find_all("tr"):
        cols = row.find_all("td")
        data.append([ele.text.strip() for ele in cols])
    df = pd.DataFrame(data)
    df.columns = titles
    return df
