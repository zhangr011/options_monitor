# encoding: UTF-8

import xml.etree.ElementTree as ET
import pandas as pd
from .logger import logger


#----------------------------------------------------------------------
# According to: https://stackoverflow.com/questions/50774222/python-extracting-xml-to-dataframe-pandas

# ----------------------------------------------------------------------
def xml_to_pandas_dataframe(xml_data, columns: list, row_name: str):
    '''get xml.etree root, the columns and return Pandas DataFrame'''
    parser = ET.XMLParser()
    parser.feed(xml_data)
    root = parser.close()
    df = None
    try:
        def get_node_value(row, key):
            node = row.find(key)
            if node is not None:
                return node.text
            return None
        rows = root.findall('.//{}'.format(row_name))
        xml_data = [[get_node_value(row, c) for c in columns] for row in rows]
        df = pd.DataFrame(xml_data, columns = columns)
    except Exception as e:
        logger.error(f'[xml_to_pandas] Exception: {e}.')
    return df

# row_name = 'row'
# columns = ['ID', 'Text', 'CreationDate']
# root = et.parse(path)
# df = xml_to_pandas(root, columns, row_name)
