from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, OrdinalEncoder, StandardScaler


class Scaler:
    def __init__(self, scaler: Any, transform_columns: list[str]) -> None:
        self.scaler = scaler
        self.transform_columns = transform_columns

    def _check_missing(self, df: pd.DataFrame) -> None:
        missing_columns = [col for col in self.transform_columns if col not in df.columns]
        if missing_columns:
            msg = f"Columns {missing_columns} not found."
            raise ValueError(msg)

    def fit(self, df: pd.DataFrame) -> None:
        self._check_missing(df)

        self.scaler.fit(df[self.transform_columns])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._check_missing(df)

        df[self.transform_columns] = self.scaler.transform(df[self.transform_columns])

        return df

    def save(self, root: str | Path) -> None:
        scaler_root = Path(root) / "scaler"
        scaler_root.mkdir(parents=True, exist_ok=True)

        file_name = scaler_root / f"{self.__class__.__name__}.bin"

        joblib.dump(self.scaler, file_name)

    def load(self, root: str | Path) -> None:
        scaler_root = Path(root) / "scaler"
        file_name = scaler_root / f"{self.__class__.__name__}.bin"

        if not file_name.exists():
            return

        self.scaler = joblib.load(file_name)


class DBStandardScaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ) -> None:
        scaler = StandardScaler(**kwargs)

        super().__init__(scaler, transform_columns)


class DBMinMaxScaler(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ) -> None:
        scaler = MinMaxScaler(**kwargs)

        super().__init__(scaler, transform_columns)


class DBOrdinalEncoder(Scaler):
    def __init__(
        self,
        transform_columns: list[str],
        **kwargs,
    ) -> None:
        scaler = OrdinalEncoder(**kwargs)

        super().__init__(scaler, transform_columns)
