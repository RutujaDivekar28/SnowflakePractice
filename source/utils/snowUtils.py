import snowflake.connector

class SnowflakeConnector:
    def __init__(self, account, user, password, warehouse, database, schema, role):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

    def connect(self):
        self.connection = snowflake.connector.connect(
            user = self.user,
            account = self.account,
            password = self.password,
            warehouse = self.warehouse,
            database = self.database,
            schema = self.schema,
            role = self.role
        )
        self.cursor = self.connection.cursor()


    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def close_connection(self):
        self.cursor.close()
        self.connection.close()


account = "LOOJADV-ZX72037"
user = "RUTUJADIVEKAR"
password = "Rutuja123"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"

sf_connector = SnowflakeConnector(
    account = account,
    user = user,
    password = password,
    warehouse = warehouse,
    database = database,
    schema = schema,
    role = role
)

sf_connector.connect()

query = 'select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER limit 10'

result = sf_connector.execute_query(query)

print(result)

sf_connector.close_connection()