from sqlalchemy import create_engine

class ConnectionFactory:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def create_connection(self):
        self.connection = create_engine(self.connection_string)