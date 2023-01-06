import dremio_caller as dc


hostname = "dremio.lid-prod.aws-eu1.energy.local"  # change 'prod' to 'stag' or 'dev' if you're running the code in those environments.
username = "UI758624"
token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA=="
client = dc.DremioFlightConnection(username=username,
                                password=token,
                                hostname=hostname,
                                port=32010)

 

# Example Query
query = "SELECT * FROM Community.Calendars.Years_Calendars"
py_table = client.execute_query(sqlquery=query)
print(py_table)