import itertools
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("agg")
import matplotlib.pylab as plt
import statsmodels.api as sm
from matplotlib.pylab import rcParams
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.tsa.arima_model import ARIMA
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

rcParams['figure.figsize'] = 15, 6

"""
- 1 parte:

data = pd.read_csv('csv_files/debs_consumo_agregado_noz.csv')
print(data.head)
print('\n Data types:')
print(data.dtypes)
"""

data = pd.read_csv('csv_files/debs_consumo_agregado_noz.csv',
                    parse_dates=['Datetime2'],
                    index_col='Datetime2')

# Remover a coluna do indice autogerado
data = data.drop('Unnamed: 0', 1)
# Criando um objeto tipo Series
ts = data['V2'] 

decomposition = sm.tsa.seasonal_decompose(ts, model='additive')
#fig = decomposition.plot()
#plt.show()

def test_stationarity(timeseries):
    print('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print(dfoutput)

#test_stationarity(ts)

model = ARIMA(ts, order=(1, 0, 0))  
results_ARIMA = model.fit(disp=-1)  
plt.plot(ts)
plt.plot(results_ARIMA.fittedvalues, color='red')
plt.title('RSS: %.4f'% sum((results_ARIMA.fittedvalues-ts)**2))
plt.show()

predictions_ARIMA_diff = pd.Series(results_ARIMA.fittedvalues, copy=True)
print(predictions_ARIMA_diff.head())
print("#############")
print(ts.head())

predictions_ARIMA_diff_cumsum = predictions_ARIMA_diff.cumsum()
print(predictions_ARIMA_diff_cumsum.head())

