import dremio_caller as dc
import pandas as pd

# username = "UI758624"
# token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA==" # Could also be PW, but not recommended. You can also get these from a secret/file you configure yourself.
# client = dc.DremioFlightConnection(username, token)
 
# # Example Query                               
# query =  """SELECT * FROM Core.Preparation.MIX.RAW.ERCOT.ERCOT_SETTLEMENT_PRICES WHERE CurveKey in ('yyz0b', 'qmv3t','cnv2v','st27y','ifui5','h4igp','yd2w4') and ReferenceDate > '2020-01-01'"""
# pd_table = client.run_dremio_flight_query(query)

# print(pd_table)


class dremio_mix:
    
   def __init__(self):
        
       self.username = "UI758624"
       self.token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA=="
       self.client = dc.DremioFlightConnection(self.username, self.token)
        
   def get_hub_spp(self):
        
       self.query = """SELECT CurveKey, TargetDate,Settlement 
       FROM Core.Preparation.MIX.RAW.ERCOT.ERCOT_SETTLEMENT_PRICES 
       WHERE CurveKey in ('qmv3t','st27y','h4igp','yd2w4') and TargetDate > '2022-01-01'"""
        
       pd_table = self.client.run_dremio_flight_query(self.query)
       
       pd_table['CurveKey'] = pd_table['CurveKey'].replace(['qmv3t','st27y','h4igp','yd2w4'],['HB_Houston','HB_North','HB_South','HB_West'])
       
       pd_table['hour'] = pd.to_datetime(pd_table['TargetDate']).dt.hour + 1
       
       pd_table['date'] = pd.to_datetime(pd_table['TargetDate']).dt.date
       
       
        
       return pd_table.groupby(['CurveKey', 'date','hour'])['Settlement'].mean().reset_index()
    

a = dremio_mix().get_hub_spp()
print(a)     