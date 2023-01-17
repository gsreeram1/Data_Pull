import pandas as pd
import urllib
from sqlalchemy import create_engine
import pandas as pd
import dremio_caller as dc
import datetime


class dremio_data:

        def __init__(self):

                self.username = "UI758624"
                self.token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA=="

                self.client = dc.DremioFlightConnection(self.username, self.token)
                

        def get_hub_spp(self):

                query = """SELECT CurveKey, TargetDate,Settlement 
                FROM Core.Preparation.MIX.RAW.ERCOT.ERCOT_SETTLEMENT_PRICES 
                WHERE CurveKey in ('qmv3t','st27y','h4igp','yd2w4') and TargetDate > '2021-01-01'"""
                
                pd_table = self.client.run_dremio_flight_query(query)

                pd_table['CurveKey'] = pd_table['CurveKey'].replace(['qmv3t','st27y','h4igp','yd2w4'],['HB_Houston','HB_North','HB_South','HB_West'])

                pd_table['hour'] = pd.to_datetime(pd_table['TargetDate']).dt.hour + 1

                pd_table['date'] = pd.to_datetime(pd_table['TargetDate']).dt.date      
                
                df = pd_table.groupby(['CurveKey', 'date','hour'])['Settlement'].mean().reset_index()

                return df

        def get_ercot_wind_forecast(self):

                query =  "SELECT * FROM Core.Preparation.S3.\"Team_US\".\"POWER\".ERCOT.\"ERCOT_Wind_ST_Fwd\" "

                pd_table = self.client.run_dremio_flight_query(query)

                return pd_table

        def get_ercot_solar_forecast(self):

                query =  "SELECT * FROM Core.Preparation.S3.\"Team_US\".\"POWER\".ERCOT.\"ERCOT_Solar_ST_Fwd\" "

                pd_table = self.client.run_dremio_flight_query(query)

                return pd_table

        def get_ercot_wind_actuals(self):

                query = """SELECT * from Core.Preparation.MIX.RAW.ERCOT.ERCOT_WIND_GENERATION WHERE CurveKey in ('7mg32','a3kjb','2n75s','xf4zy')"""

                pd_table = self.client.run_dremio_flight_query(query)

                pd_table['CurveKey'] = pd_table['CurveKey'].replace(['7mg32','a3kjb','2n75s','xf4zy'],['HB_North','HB_South','HB_West','Total'])

                return pd_table
    
       
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

        def get_DAM_hub_spp(self):

                query = """select ERCOT_Prices.PCID,ERCOT_Prices.Value,ERCOT_Prices.StartTime,ERCOT_PriceCurves.SettlementPointName,ERCOT_PriceCurves.Interval  
                from ERCOT_Prices 
                LEFT OUTER JOIN 
                ERCOT_PriceCurves on ERCOT_Prices.PCID = ERCOT_PriceCurves.PCID where ERCOT_PriceCurves.Interval = 60 and ERCOT_PriceCurves.MarketType = 'DAM' and SettlementPointName LIKE 'HB_%'
                """
        
                df = pd.read_sql_query(query,self.engine)
        
                return df 

        def get_RT_hub_spp(self):

                query = """select ERCOT_Prices.PCID,ERCOT_Prices.Value,ERCOT_Prices.StartTime,ERCOT_PriceCurves.SettlementPointName,ERCOT_PriceCurves.Interval  
                from ERCOT_Prices 
                LEFT OUTER JOIN 
                ERCOT_PriceCurves on ERCOT_Prices.PCID = ERCOT_PriceCurves.PCID where ERCOT_PriceCurves.Interval = 15 and ERCOT_PriceCurves.MarketType = 'RTM' and SettlementPointName LIKE 'HB_%'
                """
        
                df = pd.read_sql_query(query,self.engine)
        
                return df

        def get_intermittent_forecast(self):

                query = """SELECT ERCOT_Forecasts.FCID, ERCOT_Forecasts.StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (44, 41, 38, 35, 32, 108,33,36,39,42,45,109)
                """
        
                df = pd.read_sql_query(query,self.engine)


                df['FCID'] = df['FCID'].replace([32,33,35,36,38,39,41,42,44,45,108,109],['PANHANDLE','PANHANDLE','COASTAL','COASTAL','SOUTH','SOUTH','WEST','WEST','NORTH','NORTH','SYSTEM_WIDE','SYSTEM_WIDE'])
        
                return df

        def get_intermittent_actuals(self):

                query = """SELECT ERCOT_Forecasts.FCID, ERCOT_Forecasts.StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (22,23,24,25,26,27,46,106)
                """
        
                df = pd.read_sql_query(query,self.engine)

                dict = {22:'SYSTEM_WIDE',23:'PANHANDLE',24:'COASTAL',25:'SOUTH',26:'WEST',27:'NORTH',46:'SYSTEM_WIDE',106:'SYSTEM_WIDE'}

                df['FCID'] = df['FCID'].map(dict)
        
                return df

        
        def get_temp_forecasts(self):

                query = """SELECT ERCOT_Forecasts.FCID, ERCOT_Forecasts.StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (1,2,3,4,5,6,7,8)
                """
        
                df = pd.read_sql_query(query,self.engine)

                dict = {1:'Coast',2:'East',3:'Far_west',4:'North',5:'North_central',6:'South_central',7:'southern',8:'West'}

                df['FCID'] = df['FCID'].map(dict)
        
                return df

        

                        



        

        

#print(dremio_data().get_ercot_wind_actuals()['ReferenceDate'].min())