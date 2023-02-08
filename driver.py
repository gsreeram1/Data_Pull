import time
import data_extract as de
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import strategy as s

#start_time = time.time()


#print(s.strat_1().custom_allocation_forward_looking(start_date ='2019-01-01' ,days_before_settlement = 14, dart_volume = -100, futures_volume = -100, include_uri = False, include_outliers = False))
print(s.strat_1().forward_looking_short(start_date ='2019-01-01' ,days_before_settlement = 14, dart_volume = -100, futures_volume = -100, include_uri = False, include_outliers = False))

#print("--- %s seconds ---" % (time.time() - start_time))