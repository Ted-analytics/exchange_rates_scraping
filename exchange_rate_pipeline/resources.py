from dagster import ConfigurableResource
from clickhouse_driver import Client
import pandas as pd
from datetime import datetime, date

class ClickHouseResource(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_client(self):
        return Client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def create_table_if_not_exists(self):
        client = self.get_client()
        client.execute("""
            CREATE TABLE IF NOT EXISTS daily_exchange_rates(
                currency String,
                buying_rate Float64,
                average_rate Float64,
                selling_rate Float64,
                rate_date Date,
                is_actual_rate Bool,
                loaded_at DateTime DEFAULT now(),
                source_update_time DateTime
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (rate_date, currency)
        """)

    def get_latest_rates(self) -> pd.DataFrame:
        """Retrieve the most recent rates for each currency"""
        client = self.get_client()
        query = """
            SELECT 
                currency,
                buying_rate,
                average_rate,
                selling_rate,
                rate_date,
                source_update_time
            FROM daily_exchange_rates
            WHERE (currency, rate_date) IN (
                SELECT 
                    currency,
                    MAX(rate_date)
                FROM daily_exchange_rates
                GROUP BY currency
            )
        """
        result = client.execute(query)
        if result:
            df = pd.DataFrame(
                result,
                columns=['currency', 'buying_rate', 'average_rate', 'selling_rate', 'rate_date', 'source_update_time']
            )
            return df
        return None

    def load_dataframe(self, df: pd.DataFrame):
        client = self.get_client()
        data = df.to_dict('records')
        client.execute(
            """
            INSERT INTO daily_exchange_rates 
            (currency, buying_rate, average_rate, selling_rate, rate_date, is_actual_rate, source_update_time)
            VALUES
            """,
            data
        )
    def execute(self, query):
        """Execute a query without returning results"""
        client = self.get_client()
        return client.execute(query) 
    def query_df(self, query):
        """Execute a query and return results as DataFrame"""
        client = self.get_client()
        result = client.execute(query)
        if result:
            # Attempt to infer column names from the query
            columns = None
            if "SELECT" in query.upper() and "FROM" in query.upper():
                select_part = query.upper().split("FROM")[0].replace("SELECT", "").strip()
                columns = [col.strip().split(" AS ")[-1] if " AS " in col.strip() else col.strip() 
                          for col in select_part.split(",")]

            # Create DataFrame with column names if available
            if columns and len(columns) == len(result[0]):
                return pd.DataFrame(result, columns=columns)
            return pd.DataFrame(result)
        return pd.DataFrame()   