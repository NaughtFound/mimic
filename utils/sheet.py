import os
from typing import Callable, Literal, Union

import pandas as pd
from .scaler import Scaler
from .db import DuckDB, Query


class Sheet:
    def __init__(
        self,
        root: str,
        db: DuckDB,
        table_name: str,
        columns: dict[str, str],
        id_column: str,
        scaler: list[Scaler] = None,
        table_fields: dict[str, str] = None,
        transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
        drop_table: bool = True,
        force_insert: bool = False,
    ):
        self.root = root
        self.db = db
        self.table_name = table_name
        self.columns = columns
        self.id_column = id_column
        self.scaler = scaler
        self.transform = transform
        self.drop_table = drop_table
        self.force_insert = force_insert

        if table_fields is None:
            table_fields = columns

        self.table_fields = table_fields

        os.makedirs(self.root, exist_ok=True)

        self.source_csv_path = os.path.join(self.root, f"{table_name}.csv")

        if self.drop_table:
            self._drop_table()

        self._create_table()

    def _create_table(self):
        query = SheetQuery.create_table(self)
        self.db.exec(query)

    def _drop_table(self):
        query = SheetQuery.drop_table(self)
        self.db.exec(query)

    def _insert_data(self, csv_path: str):
        query = SheetQuery.copy_csv(self, csv_path)
        self.db.exec(query)

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.transform is not None:
            df = self.transform(df)

        if self.scaler is None:
            return df

        for s in self.scaler:
            df = s.transform(df)

        return df

    def load_csv(self):
        csv_path = os.path.join(self.root, "transformed", f"{self.table_name}.csv")

        if os.path.exists(csv_path) and not self.force_insert:
            if self.drop_table:
                self._insert_data(csv_path)
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


class SheetQuery(Query):
    def __init__(self, query: Union[str, list[str]]):
        if type(query) is str:
            query = [query]

        self.query = query

    def _add_query(
        self,
        query: Union[str, list[str]],
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(query) is list:
            query = " ".join(query)

        if inplace:
            self.query.append(query)
            return self

        sq = SheetQuery(self.query.copy())
        sq.query.append(query)

        return sq

    def parse(self) -> str:
        final_query = " ".join(self.query) + ";"

        return final_query

    def find_by_row_id(
        self,
        row_id: Union[int, list[int]],
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(row_id) is int:
            row_id = [row_id]

        query = f"SELECT * FROM ({' '.join(self.query)}) WHERE row_num IN ({', '.join(map(str, row_id))})"

        if inplace:
            self.query = [query]
            return self
        else:
            return SheetQuery(query)

    def find_by_id(
        self,
        column_id: str,
        id: Union[str, list[str]],
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(id) is str:
            id = [id]

        query = f"WHERE {column_id} IN ({', '.join(id)})"

        return self._add_query(query, inplace)

    def join(
        self,
        l_sheet: Sheet,
        r_sheet: Sheet,
        mode: Literal["left", "right", "natural"] = "natural",
        inplace: bool = True,
    ) -> "SheetQuery":
        query = [f"{mode.upper()} JOIN {r_sheet.table_name}"]

        if mode == "left" or mode == "right":
            query.append(
                f"ON {l_sheet.table_name}.{l_sheet.id_column}={r_sheet.table_name}.{r_sheet.id_column}"
            )

        return self._add_query(query, inplace)

    @staticmethod
    def select(
        sheet: Sheet,
        columns: Union[str, list[str]] = "*",
    ) -> "SheetQuery":
        if type(columns) is list:
            columns = ",".join(columns)

        query = [
            f"SELECT row_number() OVER () - 1 AS row_num, {columns} FROM {sheet.table_name}"
        ]

        return SheetQuery(query)

    @staticmethod
    def count(sheet: Sheet) -> "SheetQuery":
        query = [f"SELECT COUNT(*) FROM {sheet.table_name}"]

        return SheetQuery(query)

    @staticmethod
    def create_table(sheet: Sheet) -> "SheetQuery":
        columns_str = ", ".join(
            f"{col} {dtype}" for col, dtype in sheet.table_fields.items()
        )
        create_query = f"CREATE TABLE IF NOT EXISTS {sheet.table_name} ({columns_str})"

        return SheetQuery(create_query)

    @staticmethod
    def drop_table(sheet: Sheet) -> "SheetQuery":
        drop_query = f"DROP TABLE IF EXISTS {sheet.table_name}"

        return SheetQuery(drop_query)

    @staticmethod
    def copy_csv(sheet: Sheet, csv_path: str) -> "SheetQuery":
        copy_query = (
            f"COPY {sheet.table_name} FROM '{csv_path}' (FORMAT CSV, HEADER TRUE)"
        )

        return SheetQuery(copy_query)

    @staticmethod
    def empty() -> "SheetQuery":
        return SheetQuery([])
