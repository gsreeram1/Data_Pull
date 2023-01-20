import data_extract as de
import time
import pandas as pd
import matplotlib.pyplot as plt
import time


start_time = time.time()


d = de.ACDB().get_ICE_North_Prices()

plt.plot(d['date'], d['SettlementPrices'])
plt.figure(figsize=(25, 25))
plt.show()

#print(de.dremio_data().get_ercot_wind_forecast()['delivery_date'].min())



print("--- %s seconds ---" % (time.time() - start_time))