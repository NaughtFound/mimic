import os
from pathlib import Path
from typing import Any, Callable, Literal, Union

import pandas as pd
from .scaler import Scaler
from .db import DuckDB, Query


class Sheet:
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        table_name: str,
        file_name: str,
        columns: dict[str, str],
        id_column: str,
        scaler: list[Scaler] = None,
        table_fields: dict[str, str] = None,
        transform: Callable[[DuckDB, pd.DataFrame], pd.DataFrame] = None,
        drop_table: bool = True,
        force_insert: bool = False,
    ):
        self.root = root
        self.db = db
        self.table_name = table_name
        self.file_name = file_name
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

        self.source_csv_path = os.path.join(self.root, self.file_name)

        if self.drop_table:
            self._drop_table()

        self._create_table()
        self._load_scalers()

    def _load_scalers(self):
        for s in self.scaler:
            s.load(self.root)

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
            df = self.transform(self.db, df)

        if self.scaler is None:
            return df

        for s in self.scaler:
            s.fit(df)
            s.save(self.root)

        return df

    def load_csv(self):
        csv_path = os.path.join(self.root, "transformed", self.file_name)

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

        df[self.table_fields.keys()].to_csv(csv_path, index=False)

        self._insert_data(csv_path)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for s in self.scaler:
            df = s.transform(df)
        return df


class SheetJoinCondition:
    def __init__(
        self,
        l_sheet: Sheet,
        r_sheet: Sheet,
        columns: tuple[str] = None,
        mode: Literal["left", "right", "natural", "semi", "inner"] = "natural",
    ):
        if columns is None:
            columns = (l_sheet.id_column, r_sheet.id_column)

        self.l_sheet = l_sheet
        self.r_sheet = r_sheet
        self.l_column, self.r_column = columns
        self.mode = mode

    def prase(self):
        if self.l_column == self.r_column:
            return f"USING ({self.l_column})"

        return f"ON ({self.l_sheet.table_name}.{self.l_column}={self.r_sheet.table_name}.{self.r_column})"

    @property
    def _mode(self):
        return self.mode.upper()

    @property
    def _table_name(self):
        return self.r_sheet.table_name


class SheetQuery(Query):
    def __init__(self, query: Union[str, list[str]]):
        if type(query) is str:
            query = [query]

        self.query = query

    @staticmethod
    def _parse_column(column: str) -> str:
        parts = column.split(".")

        if " " in parts[-1]:
            parts[-1] = f'"{column}"'

        return ".".join(parts)

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

    def copy(self) -> "SheetQuery":
        sq = SheetQuery.empty()
        sq.query = self.query.copy()

        return sq

    def parse(self) -> str:
        final_query = " ".join(self.query) + ";"

        return final_query

    def where(
        self,
        condition: Union[str, list[str]],
        operator: Literal["and", "or"] = "and",
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(condition) is str:
            condition = [condition]

        contains_where = any(s.lower().startswith("where") for s in self.query)

        query = []

        for c in condition:
            if contains_where:
                query.append(f"{operator.upper()} {c}")
            else:
                query.append(f"WHERE {c}")
                contains_where = True

        return self._add_query(query, inplace)

    def find_by_row_id(
        self,
        row_id: Union[int, list[int]],
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(row_id) is int:
            row_id = [row_id]

        query = f"SELECT * FROM ({' '.join(self.query)})"
        condition = f"row_num IN ({','.join(map(str, row_id))})"

        if inplace:
            self.query = [query]
            return self.where(condition, inplace=inplace)
        else:
            return SheetQuery(query).where(condition, inplace=inplace)

    def find_by_id(
        self,
        column_id: str,
        id: Union[str, list[str]],
        inplace: bool = True,
    ) -> "SheetQuery":
        if type(id) is str:
            id = [id]

        condition = f"{SheetQuery._parse_column(column_id)} IN ({','.join(map(lambda col: f"'{col}'", id))})"

        return self.where(condition, inplace=inplace)

    def join(
        self,
        condition: SheetJoinCondition,
        inplace: bool = True,
    ) -> "SheetQuery":
        query = [f"{condition._mode} JOIN {condition._table_name}"]

        if condition.mode != "natural":
            query.append(condition.prase())

        return self._add_query(query, inplace)

    def limit(
        self,
        limit: int,
        offset: int = None,
        inplace: bool = True,
    ) -> "SheetQuery":
        query = [f"LIMIT {limit}"]

        if offset is not None:
            query.append(f"OFFSET {offset}")

        return self._add_query(query, inplace)

    @staticmethod
    def select(
        sheet: Sheet,
        columns: Union[str, list[str]] = "*",
    ) -> "SheetQuery":
        if type(columns) is list:
            columns = ",".join(map(lambda col: SheetQuery._parse_column(col), columns))

        query = [
            f"SELECT row_number() OVER () - 1 AS row_num, {columns} FROM {sheet.table_name}"
        ]

        return SheetQuery(query)

    @staticmethod
    def count(
        sheet: Sheet,
        columns: Union[Literal["*"], list[str]] = "*",
    ) -> "SheetQuery":
        count = []

        if columns == "*":
            count.append("COUNT(*)")

        else:
            for column in columns:
                count.append(
                    f"COUNT({SheetQuery._parse_column(column)}) AS {SheetQuery._parse_column(column)}"
                )

        query = [f"SELECT {','.join(count)} FROM {sheet.table_name}"]

        return SheetQuery(query)

    @staticmethod
    def update(sheet: Sheet, fields: dict[str, Any]) -> "SheetQuery":
        update = []

        for f in fields:
            update.append(f"{SheetQuery._parse_column(f)}={fields[f]}")

        query = f"UPDATE {sheet.table_name} SET {','.join(update)}"

        return SheetQuery(query)

    @staticmethod
    def create_table(sheet: Sheet) -> "SheetQuery":
        columns_str = ",".join(
            f"{SheetQuery._parse_column(col)} {dtype}"
            for col, dtype in sheet.table_fields.items()
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
