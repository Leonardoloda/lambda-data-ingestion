from sqlalchemy import create_engine, URL


class ConnectionFactory:
    def __init__(
        self, username: str, password: str, database: str, port: str, host: str
    ):
        self.connection_string = self.create_connection_string(
            username=username,
            password=password,
            database=database,
            port=port,
            host=host,
        )

    def create_connection_string(
        self, username: str, password: str, database: str, port: str, host: str
    ) -> URL:
        return URL(
            username=username,
            password=password,
            database=database,
            port=port,
            host=host,
        )

    def create_connection(self):
        return create_engine(self.connection_string)
