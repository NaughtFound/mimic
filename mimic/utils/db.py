from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


class Query(ABC):
    @abstractmethod
    def parse(self) -> str:
        pass


class DuckDB:
    def __init__(
        self,
        root: str | Path,
        db_name: str,
    ) -> None:
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        db_path = root / db_name
        self.conn = duckdb.connect(database=db_path, read_only=False)

    def close(self) -> None:
        self.conn.close()

    def exec(self, query: Query) -> None:
        query_str = query.parse()

        self.conn.execute(query_str)

    def fetch_df(self, query: Query) -> pd.DataFrame:
        query_str = query.parse()

        return self.conn.execute(query_str).fetch_df()

    def fetch_one(self, query: Query) -> tuple[Any, ...] | None:
        query_str = query.parse()

        return self.conn.execute(query_str).fetchone()

    def fetch_all(self, query: Query) -> list:
        query_str = query.parse()

        return self.conn.execute(query_str).fetchall()
