import os
from typing import Callable, Literal, Union
import duckdb
import pandas as pd
from utils.scaler import Scaler


class Sheet:
    def __init__(
        self,
        root: str,
        db_name: str,
        table_name: str,
        columns: dict[str, str],
        id_column: str,
        scaler: list[Scaler] = None,
        table_fields: dict[str, str] = None,
        transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
        train: bool = True,
        drop_table: bool = True,
        force_insert: bool = False,
    ):
        self.root = root
        self.table_name = table_name
        self.columns = columns
        self.id_column = id_column
        self.scaler = scaler
        self.transform = transform
        self.train = train
        self.force_insert = force_insert

        if table_fields is None:
            table_fields = columns

        self.table_fields = table_fields

        os.makedirs(self.root, exist_ok=True)

        self.source_csv_path = os.path.join(self.root, f"{table_name}.csv")
        self.db_path = os.path.join(self.root, db_name)

        self.connection = duckdb.connect(database=self.db_path, read_only=False)

        if drop_table:
            self._drop_table()

        self._create_table()

    def _create_table(self):
        columns_str = ", ".join(
            f"{col} {dtype}" for col, dtype in self.table_fields.items()
        )
        create_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_str});"
        self.connection.execute(create_query)

    def _drop_table(self):
        drop_query = f"DROP TABLE IF EXISTS {self.table_name};"
        self.connection.execute(drop_query)

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.transform is not None:
            df = self.transform(df)

        if self.scaler is None:
            return df

        for s in self.scaler:
            df = s.transform(df)

        return df

    def _insert_data(self, csv_path: str):
        insert_query = (
            f"COPY {self.table_name} FROM '{csv_path}' (FORMAT CSV, HEADER TRUE);"
        )
        self.connection.execute(insert_query)

    def load_csv(self):
        csv_path = os.path.join(self.root, "transformed", f"{self.table_name}.csv")

        if os.path.exists(csv_path) and not self.force_insert:
            return

        df = pd.read_csv(
            self.source_csv_path,
            usecols=self.columns.keys(),
            dtype=self.columns,
        )

        if self.id_column not in df.columns:
            raise ValueError(f"CSV file must contain an '{self.id_column}' column.")

        df = self._transform_data(df)

        os.makedirs(os.path.join(self.root, "transformed"), exist_ok=True)

        df.to_csv(csv_path, index=False)

        self._insert_data(csv_path)

    def close(self):
        self.connection.close()

    def exec(self, query: "SheetQuery"):
        df = self.connection.execute(query.parse()).fetch_df()
        return df


class SheetQuery:
    def __init__(self, query: Union[str, list[str]]):
        if type(query) is str:
            query = [query]

        self.query = query

    def parse(self) -> str:
        final_query = " ".join(self.query) + ";"

        return final_query

    def find_by_row_id(self, row_id: int) -> "SheetQuery":
        self.query.append(f"LIMIT 1 OFFSET {row_id}")

        return self

    def find_by_id(self, column_id: str, id: str) -> "SheetQuery":
        self.query.append(f"WHERE {column_id}={id} LIMIT 1")

        return self

    @staticmethod
    def select(
        sheet: Sheet,
        columns: Union[str, list[str]] = "*",
    ) -> "SheetQuery":
        if type(columns) is list:
            columns = ",".join(columns)

        query = [f"SELECT {columns} FROM {sheet.table_name}"]

        return SheetQuery(query)

    @staticmethod
    def join(
        l_sheet: Sheet,
        r_sheet: Sheet,
        columns: Union[str, list[str]] = "*",
        mode: Literal["left", "right", "natural"] = "natural",
    ) -> "SheetQuery":
        sq = SheetQuery.select(l_sheet, columns)

        if mode == "left" or mode == "right":
            sq.query.append(
                f"{mode.upper()} JOIN {r_sheet.table_name} ON {l_sheet.table_name}.{l_sheet.id_column}={r_sheet.table_name}.{r_sheet.id_column}"
            )

        if mode == "natural":
            sq.query.append(f"{mode.upper()} JOIN {r_sheet.table_name}")

        return sq
