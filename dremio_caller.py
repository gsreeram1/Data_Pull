from dataclasses import dataclass
from typing import Any, List, Tuple, Union, Dict
 
import logging, ssl, codecs
import pandas as pd
import pyarrow as pa
 
from pyarrow import flight
 
# For debug purposes
logger = logging.getLogger(__name__)
 
 
class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that extracts the bearer token from
    the authorization header returned by the Dremio
    Flight Server Endpoint.
    Parameters
    ----------
    factory : ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an
        authorization header with bearer token is
        returned by the Dremio server.
    """
 
    def __init__(self, factory):
        self.factory = factory
 
    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)
        if not authorization_header:
            logger.exception('Did not receive authorization header back from server.')
            raise Exception('Did not receive authorization header back from server.')
        self.factory.set_call_credential([
            b'authorization', authorization_header[0].encode('utf-8')])
 
 
class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    
    def __init__(self):
        self.call_credential = []
 
    def start_call(self, info) -> DremioClientAuthMiddleware:
        return DremioClientAuthMiddleware(self)
 
    def set_call_credential(self, call_credential) -> None:
        self.call_credential = call_credential
 
 
@dataclass           
class DremioFlightConnection:
    """
    class to set up an arrow flight client, perform authentication, and provide functions for retrieving the schema and data
    """
    username: str
    password: str
    hostname: str = "dremio.lid-prod.aws-eu1.energy.local"
    port: str =  "32010"
    certs: Union[str, bytes, Any] = "win-keychain"
    disable_server_verification: bool = True
    # If routing tag is not assigned to any queue, request will be
    # sent to the default dremio queue
    default_dremio_routing_tag: str = b"lid-toolbox-default-tag"
 # type: ignore    # Routing queue has to exist. Default: Dremio chooses
    # self.default_dremio_routing_queue = b"High Cost User Queries"
 
    def __post_init__(self) -> None:
        """As part of construction, create the FlightClient and authenticate"""
        logger.info("Post-init of FlightConnection commencing")
        self.client: flight.FlightClient = self._make_client()
        self.token: Tuple[str, str] = self._get_client_token(self.client)
        logger.info("Post-init of FlightConnection completed.")
 
    def _make_client(self) -> flight.FlightClient:
        """
        Creates an arrow flight client prepared to connect to the corresponding RWE LiD dremio endpoint.
        """
        connection_args = {}
        scheme = "grpc+tls"
        if self.certs:
            # TLS certificates are provided in a list of connection arguments. This can take multiple forms...
            if self.certs == "win-keychain":
                # Case 1: Default case, we try to extract certificates from the Windows keychain which is loaded by the SSL default context
                logger.info("Trying to extract the Root CA from windows keychain")
                ssc = ssl.create_default_context()
                ca_cert_list = ssc.get_ca_certs()
                # May be a bit odd but appropriate access according to the cert structure
                rwe_server_auth_ca_list = [i for i, cert in enumerate(ca_cert_list)
                                           if cert["subject"][-1][0][1] == 'RWE Server Auth Issuing CA']
                try:
                    cert_bin = ssc.get_ca_certs(True)[rwe_server_auth_ca_list[0]]
                    if len(rwe_server_auth_ca_list) == 1:
                        logger.info("Found the certificate that ought to be correct (RWE Server Auth Issuing CA)")
                    else:
                        logger.warning("Certificates extracted had no unique name")                  
                except IndexError:
                    logger.exception("No suitable certificate found in windows keychain, environment problem.")
                    raise
                # Certificate stored in base64 encoded text
                self.certs = "".join(codecs.encode(cert_bin, "base64").decode("utf-8").split())
                connection_args["tls_root_certs"] = f'-----BEGIN CERTIFICATE-----\n{self.certs}\n-----END CERTIFICATE-----'
            elif isinstance(self.certs, bytes):
                # If raw bytes are passed assume it's the byte level
                logger.info("Assuming that a cert was passed; using it directly as root certificate.")
                self.certs = "".join(codecs.encode(cert_bin, "base64").decode("utf-8").split())
                connection_args["tls_root_certs"] = f'-----BEGIN CERTIFICATE-----\n{self.certs}\n-----END CERTIFICATE-----'
            else:
                # Case 3: If a string or something elseis passed, it is
                # assumed that it is the path to the corresponding cert file
                logger.info("Assuming a path was passed, trying to load cert")
                with open(self.certs, "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
                logger.info("Certificate loaded.")
        elif self.disable_server_verification:
            # Connect to the server endpoint with server verification disabled.
            connection_args["disable_server_verification"] = self.disable_server_verification
        else:
            logger.error("Server verification enabled but no root cert provided.")
            raise ValueError("No certificates provided in an accepted form. "
                             "Trusted certificates must be provided to establish a TLS connection")
 
        client_auth_middleware = DremioClientAuthMiddlewareFactory()
        return flight.FlightClient(
            f"{scheme}://{self.hostname}:{self.port}",
            middleware=[client_auth_middleware],
            **connection_args,
        )
 
 
    def _get_client_token(self, client) -> Tuple[str, str]:
        """
        Connects to Dremio Flight server endpoint with the provided credentials and authenticates.
        """
        initial_options = flight.FlightCallOptions(
            headers=[
                (b"routing-tag", self.default_dremio_routing_tag),
                # Default: Let Dremio decide
                # (b"routing-queue", self.default_dremio_routing_queue),
            ]
        )
        # Authenticate with the server endpoint.
        bearer_token = client.authenticate_basic_token(self.username, self.password, initial_options)
        logger.info("Authentication succeeded")
        return bearer_token
     
 
    def _retry_authentication(self, function):
        """
        Function to contain the reauthentication logic for all member functions that make use of the Flight Client;
        reauthenticates the token if expired.
        """
        try:
            return function
        except flight.FlightUnauthenticatedError:
            logger.info("Authentication not valid (any more?), commencing reauthentication")
            try:
                self.token = self._get_client_token(self.client)
                return function
            except flight.FlightUnauthenticatedError:
                raise flight.FlightCancelledError("Re-authentication failure.")
 
 
    def get_table_schema(self, sql: str) -> List[Dict[str, str]]:
        """
        Return the (arrow) schema from query (i.e. the schema the resulting pa table would have)
        Predominantly for debugging
        ----
        Arguments:
        sql -- The associated SQL query
        """
        @self._retry_authentication
        def execute_schema_query():
            """Wrapper function because cannot decorate class methods with other class methods, having the self namespace
            in the decorator simplifies this greatly, however."""
            logger.info(f"Obtaining schema of Query:\n{sql}")
            client = self.client
            bearer_token = self.token
            options = flight.FlightCallOptions(headers=[bearer_token])
            schema = client.get_schema(flight.FlightDescriptor.for_command(sql), options)
            schema = [{"name": k, "type": v} for k, v in zip(schema.schema.names, schema.schema.types)]
            logger.info("Schema successfully obtained and dispatched.")
            return schema
        return execute_schema_query()
 
 
    def run_dremio_flight_query(self, sql: str, return_pandas_table: bool = True) -> Union[pd.DataFrame, pa.Table]:
        """
        Connects to Dremio Flight server endpoint with the token set up in the object.
        Subsequently runs the query based on the parameter and returns the result set.
        as pandas dataframe.
        ----
        Arguments:
        sql -- The requested SQL query
        return_pandas_table -- Trueish value means return a pandas df (default), falseish value means return pa Table
        """
        @self._retry_authentication
        def execute_query() -> Union[pd.DataFrame, pa.Table]:
            """Wrapper function because cannot decorate class methods with other class methods, having the self namespace
            in the decorator simplifies this greatly, however."""
            client = self.client
            bearer_token = self.token
            logger.info("Commencing querying process.")
            # Construct FlightDescriptor for the query result set.
            options = flight.FlightCallOptions(headers=[bearer_token])
            # Get the FlightInfo message to retrieve the Ticket corresponding
            # to the query result set.
            flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
            # Retrieve the result set as a stream of Arrow record batches.
            logger.info("Successfully obtained flight info.")
            reader = client.do_get(flight_info.endpoints[0].ticket, options)
            logger.info("FlightStreamReader created.")
            # (Default) case of returning the data as a pandas table
            if return_pandas_table:
                logger.info("Read to pa table, perform numerical conversion to np.float64")
                table = reader.read_all()
                # New schema with decimal128(36,10) -> float64 cast
                schema_prototype = []
                for field in table.schema:
                    if pa.types.is_decimal128(field.type):
                        schema_prototype.append(pa.field(field.name, pa.float64(), field.nullable, field.metadata))
                    else:
                        schema_prototype.append(field)
                new_schema = pa.schema(schema_prototype)
                logger.info("Outputting pandas with modified schema.")
                return table.cast(new_schema).to_pandas(split_blocks = True,
                                                        self_destruct = True,
                                                        date_as_object = False)
            else:
                logger.info("Outputting original pa table.")
                return reader.read_all()
         
        return execute_query()


username = "UI758624"
token = "LiCw3ErlR6e4ABCqxEjpec+ganUkZY7YGat8Q+qo+0xgBH4f6tfhkLqfvdLVoA==" # Could also be PW, but not recommended. You can also get these from a secret/file you configure yourself.
client = DremioFlightConnection(username, token)
