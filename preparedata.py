import pandas as pd

rawdata = './data/sensordata.csv'
outpath = './data/sensordata.parquet'

colnames = ['status', 'date', 'time', 'sensor']
df = pd.read_csv(rawdata, header=None, names=colnames)

df.to_parquet(outpath)
