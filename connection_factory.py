from sqlalchemy import create_engine, URL


class ConnectionFactory:
    def __init__(self, connection_config: dict):
        self.connection_string = self.create_connection_string(
            connection_config=connection_config
        )

    def create_connection_string(self, connection_config: dict) -> URL:
        return URL(
            drivername="mysql+pymysql",
            username=connection_config.get("username"),
            password=connection_config.get("password"),
            database=connection_config.get("database"),
            port=int(connection_config.get("port")),
            host=connection_config.get("host"),
            query={},
        )

    def create_connection(self):
        return create_engine(self.connection_string, echo=True)
