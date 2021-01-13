# encoding: UTF-8

import pandas as pd


# According to: https://stackoverflow.com/questions/23377533/python-beautifulsoup-parsing-table
#----------------------------------------------------------------------
def soup_to_pandas_dataframe(soup):
    tables = soup.find_all("table")
    tab = tables[0]
    head = tab.find('thead')
    titles = []
    for title in head.find_all('td'):
        titles.append(title.text.strip())
    data = []
    body = tab.find('tbody')
    for row in body.find_all("tr"):
        cols = row.find_all("td")
        data.append([ele.text.strip() for ele in cols])
    df = pd.DataFrame(data)
    df.columns = titles
    return df
