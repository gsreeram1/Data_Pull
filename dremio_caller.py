import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq

 

class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):

 

    def __init__(self):
        self.call_credential = []

 

    def start_call(self, info):
        return DremioClientAuthMiddleware(self)

 

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential

 


class DremioClientAuthMiddleware(flight.ClientMiddleware):

 

    def __init__(self, factory):
        self.factory = factory

 


class DremioFlightConnection:

 

    def __init__(self, username, password, hostname, port):

 

        self.username = username
        self.password = password
        self.hostname = hostname
        self.flightport = port
        self.default_dremio_routing_queue = b"High Cost User Queries"

 

    def _get_client(self):
        try:
            scheme = "grpc+tcp"
            connection_args = {}
            scheme = "grpc+tls"
            connection_args["disable_server_verification"] = True
            client_auth_middleware = DremioClientAuthMiddlewareFactory()
            return flight.FlightClient(
                f"{scheme}://{self.hostname}:{self.flightport}",
                middleware=[client_auth_middleware],
                **connection_args,
            )

 

        except:
            raise

 

    def _get_client_token(self, client):
        initial_options = flight.FlightCallOptions(
            headers=[
                (b"routing-queue", self.default_dremio_routing_queue),
            ]
        )

 

        bearer_token = client.authenticate_basic_token(self.username, self.password, initial_options)
        return bearer_token

 

    def execute_query(self, sqlquery):
        try:
            client = self._get_client()
            bearer_token = self._get_client_token(client)
            if sqlquery:
                options = flight.FlightCallOptions(headers=[bearer_token])
                flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sqlquery), options)
                reader = client.do_get(flight_info.endpoints[0].ticket, options)
                return reader.read_pandas()

 

        except Exception as exception:
            print(f"Exception: {repr(exception)}")
            raise

 