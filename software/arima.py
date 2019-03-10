import pandas as pd
import numpy as np
from pyramid.arima import auto_arima
#from pmdarima.arima import auto_arima
import matplotlib
#matplotlib.use("agg")
import matplotlib.pylab as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from matplotlib.pylab import rcParams

rcParams['figure.figsize'] = 15, 6

data = pd.read_csv('csv_files/debs_consumo_agregado_noz.csv',
                    parse_dates=['Datetime2'],
                    index_col='Datetime2')

data.index = pd.to_datetime(data.index)

data = data.drop('Unnamed: 0', 1)

data[pd.isnull(data['V2'])]

data.columns = ['power_consumption']

# Decomposicao da serie temporal
#result = seasonal_decompose(data, model='multiplicative')
#fig = result.plot()
#plt.show()

stepwise_model = auto_arima(data, start_p=0, start_q=0,
                           max_p=3, max_q=3, m=24,
                           start_P=2, seasonal=True,
                           d=2, D=2, trace=True,
                           error_action='ignore',  
                           suppress_warnings=True, 
                           stepwise=True,
                           stationary=True)
#print(stepwise_model.aic())

# listas de treino e teste
train = data.loc['2013-08-31':'2013-09-03']
test = data.loc['2013-09-04':]

stepwise_model.fit(train)

future_forecast = stepwise_model.predict(n_periods=32)
future_forecast = pd.DataFrame(future_forecast,index = test.index,columns=['Prediction'])
plt.plot(future_forecast)
plt.plot(data)
plt.show()
#pd.concat([test,future_forecast],axis=1).to_csv("fore.csv")

#future_forecast2 = future_forecast

#pd.concat([data,future_forecast2],axis=1).to_csv("fore2.csv")

