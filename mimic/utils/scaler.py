from typing import Any
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OrdinalEncoder


class Scaler:
    def __init__(
        self,
        scaler: Any,
        transform_columns: list[str],
        train: bool = True,
    ):
        self.scaler = scaler
        self.transform_columns = transform_columns
        self.train = train

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        missing_columns = [
            col for col in self.transform_columns if col not in df.columns
        ]
        if missing_columns:
            raise ValueError(f"Columns {missing_columns} not found.")

        if self.train:
            self.scaler.fit(df[self.transform_columns])

        df[self.transform_columns] = self.scaler.transform(df[self.transform_columns])

        return df


class Standard_Scaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        train: bool = True,
        **kwargs,
    ):
        scaler = StandardScaler(**kwargs)

        super().__init__(scaler, transform_columns, train)


class MinMax_Scaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        train: bool = True,
        **kwargs,
    ):
        scaler = MinMaxScaler(**kwargs)

        super().__init__(scaler, transform_columns, train)


class Ordinal_Encoder(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        train: bool = True,
        **kwargs,
    ):
        scaler = OrdinalEncoder(**kwargs)

        super().__init__(scaler, transform_columns, train)
