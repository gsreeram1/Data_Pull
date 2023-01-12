import pandas as pd
import urllib
from sqlalchemy import create_engine

import pandas as pd

import dremio_caller as dc



class dremio_data:

        
   def get_hub_spp(self):
       
        self.username = "UI758624"
        self.token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA=="
    
        client = dc.DremioFlightConnection(self.username, self.token)

        query = "SELECT CurveKey, TargetDate,Settlement FROM Core.Preparation.MIX.RAW.ERCOT.ERCOT_SETTLEMENT_PRICES WHERE CurveKey in ('qmv3t','st27y','h4igp','yd2w4') and TargetDate > '2021-01-01'"
        
        pd_table = client.run_dremio_flight_query(query)
       
        pd_table['CurveKey'] = pd_table['CurveKey'].replace(['qmv3t','st27y','h4igp','yd2w4'],['HB_Houston','HB_North','HB_South','HB_West'])
       
        pd_table['hour'] = pd.to_datetime(pd_table['TargetDate']).dt.hour + 1
       
        pd_table['date'] = pd.to_datetime(pd_table['TargetDate']).dt.date      
        
        df = pd_table.groupby(['CurveKey', 'date','hour'])['Settlement'].mean().reset_index()
    
        return df



    
       
class ACDB:
    
    def __init__ (self):
        
        self.server = 'acprototypeus.database.windows.net'
        self.database = 'assetcommercialus'
        self.username = 'ACDB_US_USER'
        self.password = 'JguwS9PZZ5Zwv9APZZ'

        self.params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
 
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.params)
        
    def get_ERCOT_load_actuals(self):
        
        
        query = """select FCID, StartTime, Value from ERCOT_Forecasts
        WHERE FCID IN (110,111,112,113,114,115,116,117,119,120,121,122,123)"""

        df = pd.read_sql_query(query,self.engine)
        
        df['FCID'] = df['FCID'].replace([110,111,112,113,114,115,116,117,119,120,121,122,123],['Coast','East','FAR_WEST','North','NORTH_C','Southern','SOUTH_C','West','NORTH','SOUTH','WEST','HOUSTON','TOTAL'])

        return df
   
    def get_ERCOT_load_forecasts(self):
        
       query = """select FCID, StartTime, Value from ERCOT_Forecasts WHERE FCID IN (9,10,11,12,13,14,15,16,17,18,19,20,21)"""
       
       df = pd.read_sql_query(query,self.engine)
       
       df['FCID'] = df['FCID'].replace([9,10,11,12,13,14,15,16,17,18,19,21],['North','South','West','Houston','Coast','East','FarWest','North','NorthCentral','SouthCentral','Southern','System']) 
       
       return df
        

print(dremio_data().get_hub_spp())