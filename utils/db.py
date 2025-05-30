import os
import duckdb
import pandas as pd
from abc import ABC, abstractmethod


class Query(ABC):
    @abstractmethod
    def parse(self) -> str:
        pass


class DuckDB:
    def __init__(
        self,
        root: str,
        db_name: str,
    ):
        db_path = os.path.join(root, db_name)
        self.conn = duckdb.connect(database=db_path, read_only=False)

    def close(self):
        self.conn.close()

    def exec(self, query: Query):
        query_str = query.parse()

        self.conn.execute(query_str)

    def fetch_df(self, query: Query) -> pd.DataFrame:
        query_str = query.parse()

        df = self.conn.execute(query_str).fetch_df()

        return df

    def fetch_one(self, query: Query) -> tuple:
        query_str = query.parse()

        res = self.conn.execute(query_str).fetchone()

        return res

    def fetch_all(self, query: Query) -> list:
        query_str = query.parse()

        res = self.conn.execute(query_str).fetchall()

        return res
