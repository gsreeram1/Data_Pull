import dremio_caller as dc


username = "UI758624"
token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA==" # Could also be PW, but not recommended. You can also get these from a secret/file you configure yourself.
client = dc.DremioFlightConnection(username, token)
 
# Example Query                               
query = "SELECT * FROM Core.Preparation.MIX.RAW.ERCOT.POWER_GENERATION_DATA"
pd_table = client.run_dremio_flight_query(query)

print(pd_table)