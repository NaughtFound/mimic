import os
from pathlib import Path
from typing import Any, Union
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OrdinalEncoder
import joblib


class Scaler:
    def __init__(self, scaler: Any, transform_columns: list[str]):
        self.scaler = scaler
        self.transform_columns = transform_columns

    def _check_missing(self, df: pd.DataFrame):
        missing_columns = [
            col for col in self.transform_columns if col not in df.columns
        ]
        if missing_columns:
            raise ValueError(f"Columns {missing_columns} not found.")

    def fit(self, df: pd.DataFrame):
        self._check_missing(df)

        self.scaler.fit(df[self.transform_columns])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._check_missing(df)

        df[self.transform_columns] = self.scaler.transform(df[self.transform_columns])

        return df

    def save(self, root: Union[str, Path]):
        scaler_root = os.path.join(root, "scaler")
        os.makedirs(scaler_root, exist_ok=True)

        file_name = os.path.join(scaler_root, f"{self.__class__.__name__}.bin")

        joblib.dump(self.scaler, file_name)

    def load(self, root: Union[str, Path]):
        scaler_root = os.path.join(root, "scaler")
        file_name = os.path.join(scaler_root, f"{self.__class__.__name__}.bin")

        if not os.path.exists(file_name):
            return

        self.scaler = joblib.load(file_name)


class Standard_Scaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ):
        scaler = StandardScaler(**kwargs)

        super().__init__(scaler, transform_columns)


class MinMax_Scaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ):
        scaler = MinMaxScaler(**kwargs)

        super().__init__(scaler, transform_columns)


class Ordinal_Encoder(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ):
        scaler = OrdinalEncoder(**kwargs)

        super().__init__(scaler, transform_columns)
