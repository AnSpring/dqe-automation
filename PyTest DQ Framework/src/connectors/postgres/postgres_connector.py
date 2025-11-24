
import psycopg2
import pandas as pd

class PostgresConnectorContextManager:
    def __init__(self, db_user: str, db_password: str, db_host: str, db_name: str, db_port: str):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port

        self.connection = None
        self.cursor = None

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                database=self.db_name,
                port=self.db_port
            )
            self.cursor = self.connection.cursor()
            return self
        except Exception as e:
            raise Exception(f"Postgres connection failed: {e}")

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_data_sql(self, sql: str) -> pd.DataFrame:
        """
        Executes SQL query and returns result as pandas DataFrame.
        """
        try:
            self.cursor.execute(sql)
            records = self.cursor.fetchall()
            colnames = [desc[0] for desc in self.cursor.description]
            df = pd.DataFrame(records, columns=colnames)
            return df
        except Exception as e:
            raise Exception(f"SQL execution failed: {e}")


