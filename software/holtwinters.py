from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
import pandas as pd
#import matplotlib
#matplotlib.use("agg")
import matplotlib.pyplot as plt

df = pd.read_csv('csv_files/debs_consumo_agregado_noz.csv',
                    parse_dates=['Datetime2'],
                    index_col='Datetime2')

df.index = pd.to_datetime(df.index)
df = df.drop('Unnamed: 0', 1)
df[pd.isnull(df['V2'])]
df.columns = ['power_consumption']

df.index.freq = 'H'
train, test = df.iloc[:101, 0], df.iloc[101:, 0]
model = ExponentialSmoothing(train, seasonal='add', seasonal_periods=24).fit()
pred = model.predict(start=test.index[0], end=test.index[-1])
plt.plot(pred)
plt.plot(df)
plt.show()