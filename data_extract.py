import pandas as pd
import urllib
from sqlalchemy import create_engine
import pandas as pd
import dremio_caller as dc
import datetime as dt
from datetime import datetime, timedelta


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
        
        
                query = """select FCID, Convert(datetime,Cast(StartTime As datetime),108) As StartTime, Value from ERCOT_Forecasts
                WHERE FCID IN (110,111,112,113,114,115,116,117,119,120,121,122,123)"""

                df = pd.read_sql_query(query,self.engine)
                
                df['FCID'] = df['FCID'].replace([110,111,112,113,114,115,116,117,119,120,121,122,123],['Coast','East','FAR_WEST','North','NORTH_C','Southern','SOUTH_C','West','NORTH','SOUTH','WEST','HOUSTON','TOTAL'])

                df = df.convert_dtypes()

                df = df.pivot_table('Value',['StartTime'],'FCID').reset_index()

                return df
        
        def get_ERCOT_load_forecasts(self):
        
                query = """select FCID, Convert(datetime,Cast(ERCOT_Forecasts.StartTime As datetime),108) As StartTime, Value from ERCOT_Forecasts WHERE FCID IN (9,10,11,12,13,14,15,16,17,18,19,21)"""
        
                df = pd.read_sql_query(query,self.engine)
        
                df['FCID'] = df['FCID'].replace([9,10,11,12,13,14,15,16,17,18,19,21],['North','South','West','Houston','Coast','East','FarWest','North','NorthCentral','SouthCentral','Southern','System'])

                df = df.convert_dtypes()

                df = df.pivot_table("Value",'StartTime','FCID').reset_index() 
        
                return df

        def get_DAM_hub_spp(self):

                query = """select ERCOT_Prices.PCID,ERCOT_Prices.Value,Convert(datetime,Cast(ERCOT_Prices.StartTime As datetime),108) As StartTime,ERCOT_PriceCurves.SettlementPointName,ERCOT_PriceCurves.Interval  
                from ERCOT_Prices 
                LEFT OUTER JOIN 
                ERCOT_PriceCurves on ERCOT_Prices.PCID = ERCOT_PriceCurves.PCID where ERCOT_PriceCurves.Interval = 60 and ERCOT_PriceCurves.MarketType = 'DAM' and SettlementPointName LIKE 'HB_%'
                """
        
                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                df = df.pivot_table('Value','StartTime','SettlementPointName').reset_index()
        
                return df 

        def get_RT_hub_spp(self):

                query = """select ERCOT_Prices.PCID,ERCOT_Prices.Value,Convert(datetime,Cast(ERCOT_Prices.StartTime As datetime),108) As StartTime,ERCOT_PriceCurves.SettlementPointName,ERCOT_PriceCurves.Interval  
                from ERCOT_Prices 
                LEFT OUTER JOIN 
                ERCOT_PriceCurves on ERCOT_Prices.PCID = ERCOT_PriceCurves.PCID where ERCOT_PriceCurves.Interval = 15 and ERCOT_PriceCurves.MarketType = 'RTM' and SettlementPointName LIKE 'HB_%'
                """
        
                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                df = df.pivot_table('Value','StartTime','SettlementPointName').reset_index()

                df = df.resample('H',on = 'StartTime').mean().reset_index()
        
                return df

        def get_intermittent_forecast(self):

                query = """SELECT ERCOT_Forecasts.FCID, Convert(datetime,Cast(ERCOT_Forecasts.StartTime As datetime),108) As StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (44, 41, 38, 35, 32, 108,33,36,39,42,45,109)
                """
        
                df = pd.read_sql_query(query,self.engine)


                df['FCID'] = df['FCID'].replace([32,33,35,36,38,39,41,42,44,45,108,109],['PANHANDLE','PANHANDLE','COASTAL','COASTAL','SOUTH','SOUTH','WEST','WEST','NORTH','NORTH','SYSTEM_WIDE','SYSTEM_WIDE'])

                df = df.convert_dtypes()

                df = df.pivot_table('Value',['StartTime','Type'],'FCID').reset_index()
        
                return df

        def get_intermittent_actuals(self):

                query = """SELECT ERCOT_Forecasts.FCID,Convert(datetime,Cast(ERCOT_Forecasts.StartTime As datetime),108) As StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (22,23,24,25,26,27,46,106)
                """
        
                df = pd.read_sql_query(query,self.engine)

                dict = {22:'SYSTEM_WIDE',23:'PANHANDLE',24:'COASTAL',25:'SOUTH',26:'WEST',27:'NORTH',46:'SYSTEM_WIDE_1',106:'SYSTEM_WIDE'}

                df['FCID'] = df['FCID'].map(dict)

                df = df.convert_dtypes()

                df = df.pivot_table('Value',['StartTime','Type'],'FCID').reset_index()
        
                return df

        
        def get_temp_forecasts(self):

                query = """SELECT ERCOT_Forecasts.FCID, Convert(datetime,Cast(ERCOT_Forecasts.StartTime As datetime),108) As StartTime, ERCOT_Forecasts.Value, ERCOT_ForecastCurves.Type
                from ERCOT_Forecasts
                left outer join
                ERCOT_ForecastCurves
                on ERCOT_Forecasts.FCID = ERCOT_ForecastCurves.FCID
                where ERCOT_ForecastCurves.FCID in (1,2,3,4,5,6,7,8)
                """
        
                df = pd.read_sql_query(query,self.engine)

                dict = {1:'Coast',2:'East',3:'Far_west',4:'North',5:'North_central',6:'South_central',7:'southern',8:'West'}

                df['FCID'] = df['FCID'].map(dict)

                df = df.convert_dtypes()

                df = df.pivot_table('Value','StartTime','FCID').reset_index()
        
                return df


        def get_NGLD1_prices(self):

                query= """Select *From (Select 'ICE-NGX' As CurveSource, 'HENRYHUB' As Market, Lbl, CurveID, Convert(datetime,Cast(TradeDate As datetime),108) As TradeDate, Convert(datetime,Cast(Strip As datetime),108) As Strip, Convert(datetime,Cast(ExpirationDate As datetime),108) AS ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From (
                        (Select 'Current' As Lbl, CurveID, TradeDate, Strip, ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product From
                        (Select CurveID, TradeDate, Strip, ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From
                        (Select fp.CurveID, fp.TradeDate, fp.SettlementPrice, f.Strip, f.ExpirationDate, f.Hub, f.Commodity, f.Contract, f.ContractType, f.Exchange, f.Product
                        From assetcommercialus.dbo.ICE_ForwardPrices fp
                        Left Join 
                        (Select CurveID, Strip, ExpirationDate, iceh.Hub, icecom.Commodity, icecon.Contract, icecont.ContractType, iceex.Exchange, iceproduct.Product
                        From assetcommercialus.dbo.ICE_Forwards icef
                        Left Join assetcommercialus.dbo.ICE_Hub iceh On iceh.ID = icef.Hub
                        Left Join assetcommercialus.dbo.ICE_Commodity icecom On icecom.ID = icef.Commodity
                        Left Join assetcommercialus.dbo.ICE_Contract icecon On icecon.ID = icef.Contract
                        Left Join assetcommercialus.dbo.ICE_ContractType icecont On icecont.ID = icef.ContractType
                        Left Join assetcommercialus.dbo.ICE_Exchange iceex On iceex.ID = icef.Exchange
                        Left Join assetcommercialus.dbo.ICE_Imports iceimp On (iceimp.Commodity = icecom.Commodity And iceimp.Contract = icecon.Contract And iceimp.Exchange = iceex.Exchange)
                        Left Join assetcommercialus.dbo.ICE_Product iceproduct On iceproduct.ID = icef.Product
                        ) f
                        On f.CurveID = fp.CurveID
                        ) As fp2

                        Where fp2.TradeDate > '2019-01-01'
                        And fp2.Product = 'NG LD1 Futures'
                        And fp2.Hub = 'Henry'
                        ) As tblh
                        )
                        ) As tblhenryhub) tblfinal 
                        Order By CurveSource, Market, Product, Hub, Commodity, Strip, Contract, Lbl"""

                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                data = pd.DataFrame()

                data['date'] = df['TradeDate'].unique()

                data['strip_date'] = data['date'].apply(lambda x: x.replace(day=1))
                data['strip_date'] = data['strip_date'].apply(lambda x:x + timedelta(days=32)).apply(lambda x: x.replace(day=1)) #Change time delta to 64 to get 2 months ahead prompt+1

                df_2 = data.merge(df,left_on = ['date','strip_date'], right_on = ['TradeDate','Strip'])

                return df_2


        def get_North_HR(self): #Prompt -- XRW is the ATC HR

                query = """(Select 'Current' As Lbl, * From
                        (Select CurveID, Convert(datetime,Cast(TradeDate As datetime),108) As TradeDate, Convert(datetime,Cast(Strip As datetime),108) As Strip, Convert(datetime,Cast(ExpirationDate As datetime),108) AS ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From
                        (Select fp.CurveID, fp.TradeDate, fp.SettlementPrice, f.Strip, f.ExpirationDate, f.Hub, f.Commodity, f.Contract, f.ContractType, f.Exchange, f.Product
                        From assetcommercialus.dbo.ICE_ForwardPrices fp
                        Left Join 
                        (Select CurveID, Strip, ExpirationDate, iceh.Hub, icecom.Commodity, icecon.Contract, icecont.ContractType, iceex.Exchange, iceproduct.Product
                        From assetcommercialus.dbo.ICE_Forwards icef
                        Left Join assetcommercialus.dbo.ICE_Hub iceh On iceh.ID = icef.Hub
                        Left Join assetcommercialus.dbo.ICE_Commodity icecom On icecom.ID = icef.Commodity
                        Left Join assetcommercialus.dbo.ICE_Contract icecon On icecon.ID = icef.Contract
                        Left Join assetcommercialus.dbo.ICE_ContractType icecont On icecont.ID = icef.ContractType
                        Left Join assetcommercialus.dbo.ICE_Exchange iceex On iceex.ID = icef.Exchange
                        Left Join assetcommercialus.dbo.ICE_Imports iceimp On (iceimp.Commodity = icecom.Commodity And iceimp.Contract = icecon.Contract And iceimp.Exchange = iceex.Exchange)
                        Left Join assetcommercialus.dbo.ICE_Product iceproduct On iceproduct.ID = icef.Product
                        ) f
                        On f.CurveID = fp.CurveID
                        ) As fp2

                        Where fp2.TradeDate > '2017-01-01'
                        And (fp2.Contract = 'XPR') 
                        ) As tblHRercotnorth)"""

                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                data = pd.DataFrame()

                data['date'] = df['TradeDate'].unique()

                data['strip_date'] = data['date'].apply(lambda x: x.replace(day=1))
                data['strip_date'] = data['strip_date'].apply(lambda x:x + timedelta(days=32)).apply(lambda x: x.replace(day=1)) #Change time delta to 64 to get 2 months ahead prompt+1

                df_2 = data.merge(df,left_on = ['date','strip_date'], right_on = ['TradeDate','Strip'])

                return df_2

        def get_West_HR(self): #Prompt -- XS9 is the ATC HR and XQH is on-peak

                query = """(Select 'Current' As Lbl, * From
                        (Select CurveID, Convert(datetime,Cast(TradeDate As datetime),108) As TradeDate, Convert(datetime,Cast(Strip As datetime),108) As Strip, Convert(datetime,Cast(ExpirationDate As datetime),108) AS ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From
                        (Select fp.CurveID, fp.TradeDate, fp.SettlementPrice, f.Strip, f.ExpirationDate, f.Hub, f.Commodity, f.Contract, f.ContractType, f.Exchange, f.Product
                        From assetcommercialus.dbo.ICE_ForwardPrices fp
                        Left Join 
                        (Select CurveID, Strip, ExpirationDate, iceh.Hub, icecom.Commodity, icecon.Contract, icecont.ContractType, iceex.Exchange, iceproduct.Product
                        From assetcommercialus.dbo.ICE_Forwards icef
                        Left Join assetcommercialus.dbo.ICE_Hub iceh On iceh.ID = icef.Hub
                        Left Join assetcommercialus.dbo.ICE_Commodity icecom On icecom.ID = icef.Commodity
                        Left Join assetcommercialus.dbo.ICE_Contract icecon On icecon.ID = icef.Contract
                        Left Join assetcommercialus.dbo.ICE_ContractType icecont On icecont.ID = icef.ContractType
                        Left Join assetcommercialus.dbo.ICE_Exchange iceex On iceex.ID = icef.Exchange
                        Left Join assetcommercialus.dbo.ICE_Imports iceimp On (iceimp.Commodity = icecom.Commodity And iceimp.Contract = icecon.Contract And iceimp.Exchange = iceex.Exchange)
                        Left Join assetcommercialus.dbo.ICE_Product iceproduct On iceproduct.ID = icef.Product
                        ) f
                        On f.CurveID = fp.CurveID
                        ) As fp2
                        Where fp2.TradeDate > '2017-01-01'
                        And (fp2.Contract = 'XQH') 
                        ) As tblHRercotwest)"""

                df = pd.read_sql_query(query, self.engine)

                df = df.convert_dtypes()

                data = pd.DataFrame()

                data['date'] = df['TradeDate'].unique()

                data['strip_date'] = data['date'].apply(lambda x: x.replace(day=1))
                data['strip_date'] = data['strip_date'].apply(lambda x:x + timedelta(days=32)).apply(lambda x: x.replace(day=1)) #Change time delta to 64 to get 2 months ahead prompt+1

                df_2 = data.merge(df,left_on = ['date','strip_date'], right_on = ['TradeDate','Strip'])                

                return df_2

        
        def get_ICE_North_Prices(self):

                query = """Select 'ICE-NGX' As CurveSource, 'ERCOT' As Market, Lbl, CurveID, TradeDate, CurveID, Convert(datetime,Cast(TradeDate As datetime),108) As TradeDate, Convert(datetime,Cast(Strip As datetime),108) As Strip, Convert(datetime,Cast(ExpirationDate As datetime),108) AS ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From (
                        (Select 'Current' As Lbl, * From
                        (Select CurveID, TradeDate, Strip, ExpirationDate, SettlementPrice, Hub, Commodity, Contract, ContractType, Exchange, Product
                        From
                        (Select fp.CurveID, fp.TradeDate, fp.SettlementPrice, f.Strip, f.ExpirationDate, f.Hub, f.Commodity, f.Contract, f.ContractType, f.Exchange, f.Product
                        From assetcommercialus.dbo.ICE_ForwardPrices fp
                        Left Join 
                        (Select CurveID, Strip, ExpirationDate, iceh.Hub, icecom.Commodity, icecon.Contract, icecont.ContractType, iceex.Exchange, iceproduct.Product
                        From assetcommercialus.dbo.ICE_Forwards icef
                        Left Join assetcommercialus.dbo.ICE_Hub iceh On iceh.ID = icef.Hub
                        Left Join assetcommercialus.dbo.ICE_Commodity icecom On icecom.ID = icef.Commodity
                        Left Join assetcommercialus.dbo.ICE_Contract icecon On icecon.ID = icef.Contract
                        Left Join assetcommercialus.dbo.ICE_ContractType icecont On icecont.ID = icef.ContractType
                        Left Join assetcommercialus.dbo.ICE_Exchange iceex On iceex.ID = icef.Exchange
                        Left Join assetcommercialus.dbo.ICE_Imports iceimp On (iceimp.Commodity = icecom.Commodity And iceimp.Contract = icecon.Contract And iceimp.Exchange = iceex.Exchange)
                        Left Join assetcommercialus.dbo.ICE_Product iceproduct On iceproduct.ID = icef.Product
                        ) f
                        On f.CurveID = fp.CurveID
                        ) As fp2
                        Where fp2.TradeDate > '2017-01-01'
                        And (fp2.Contract = 'END' Or fp2.Contract = 'XRT' Or fp2.Contract = 'XPA' Or fp2.Contract = 'XPB' Or fp2.Contract = 'XPC' Or fp2.Contract = 'XPD') 
                        ) As tblphysercotnorth)) As tblhenryhub"""

                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                return df

        def get_total_outages(self):

                query = """(Select Dateadd(Hour,HourEnding-1,Convert(datetime,Cast(Date As datetime),108)) As StartTime
                        , HourEnding As HE
                        , TotalResourceMWZoneSouth+TotalResourceMWZoneNorth+TotalResourceMWZoneWest+TotalResourceMWZoneHouston As TotResourceCapOut
                        , TotalIRRMWZoneSouth+TotalIRRMWZoneNorth+TotalIRRMWZoneWest+TotalIRRMWZoneHouston As TotRenewResourceCapOut
                        , TotalNewEquipResourceMWZoneSouth+TotalNewEquipResourceMWZoneNorth+TotalNewEquipResourceMWZoneWest+TotalNewEquipResourceMWZoneHouston As TotNewEquipResourceCapOut
                        , TotalResourceMWZoneHouston As TotResCapOutHouston
                        , TotalResourceMWZoneNorth As TotResCapOutNorth
                        , TotalResourceMWZoneSouth As TotResCapOutSouth
                        , TotalResourceMWZoneWest As TotResCapOutWest
                        , TotalIRRMWZoneHouston As TotRenewResourceCapOutHouston
                        , TotalIRRMWZoneNorth As TotRenewResourceCapOutNorth
                        , TotalIRRMWZoneSouth As TotRenewResourceCapOutSouth
                        , TotalIRRMWZoneWest As TotRenewResourceCapOutWest
                        , TotalNewEquipResourceMWZoneHouston As TotNEquipCapOutHouston
                        , TotalNewEquipResourceMWZoneNorth As TotNEquipCapOutNorth
                        , TotalNewEquipResourceMWZoneSouth As TotNEquipCapOutSouth
                        , TotalNewEquipResourceMWZoneWest As TotNEquipCapOutWest
                        From assetcommercialus.dbo.ERCOT_ResourceOutageCapacity)"""
                
                df = pd.read_sql_query(query,self.engine)

                df = df.convert_dtypes()

                return df
                

#print(dremio_data().get_ercot_wind_actuals()['ReferenceDate'].min())