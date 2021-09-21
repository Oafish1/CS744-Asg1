import sys

import pandas as pd


"""
TODO:
- Pull data from HDFS
- Implement spark
"""


def sort(df):
    return df.sort_values(['cca2', 'timestamp'])


fname = sys.argv[1]
data = pd.read_csv(fname)
data = sort(data)
print(data)
