import pandas as pd

rawdata = './data/HomeIntellex_1.csv'
outpath = './data/HomeIntellex_1.parquet'

colnames = ['status', 'date', 'time', 'sensor']
df = pd.read_csv(rawdata, header=None, names=colnames)

df.to_parquet(outpath)