import time
import data_extract as de
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from datetime import datetime, timedelta
import numpy as np

#start_time = time.time()


#Loading pre-processed dataframes

dart_data = de.ACDB().get_dart_data()[['StartTime','north_rt','north_da','north_dart']]
ice_data = de.ACDB().get_ice_north_custom(-14) #2nd week contract

#Resampling dart data into daily averages

dart_data = dart_data.resample('D', on  = 'StartTime').mean().reset_index()

#Merging ICE and DART prices

data = ice_data.merge(dart_data, left_on = 'trade_date', right_on = 'StartTime')

#calculating spread between DA and price 2-weeks ago


data['spread'] = data['north_da'] - data['SettlementPrice']
data['short_dart'] = data['north_dart']*16*100
data['short_spread'] = data['spread']*16*-100

#Removing URI
uri_dates = ['2021-02-11','2021-02-12','2021-02-13','2021-02-14','2021-02-15','2021-02-16','2021-02-17','2021-02-18','2021-02-19','2021-02-26']
outliers = ['2021-02-26', '2019-08-30','2021-02-24','2021-03-01','2022-02-14'] #spread greater than 500, outliers

data = data[~data['trade_date'].isin(uri_dates)]
data = data[~data['trade_date'].isin(outliers)]

#Date filter

test = data[data['trade_date']>'2022-12-01']


fig, ax = plt.subplots()

ax.plot(test['StartTime'],test['short_spread'].cumsum(), label = "2 Weeks", marker = ".", color = 'red')
ax.plot(test['StartTime'],test['short_dart'].cumsum(), label = "Day-ahead", marker = ".", color = 'green')

ax.yaxis.set_major_formatter('${x:1,.0f}')

ax.yaxis.set_tick_params(which='major', labelcolor='green',
                         labelleft=True, labelright=False)

ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))



ax.grid()
ax.legend()
plt.xticks(rotation=45)
plt.show()

#print("--- %s seconds ---" % (time.time() - start_time))