import data_extract as de
import time
start_time = time.time()


print(de.ACDB().get_NGLD1_prices())

#print(de.dremio_data().get_ercot_wind_forecast()['delivery_date'].min())



print("--- %s seconds ---" % (time.time() - start_time))