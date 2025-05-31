from pathlib import Path
from typing import Union
import torch
from torch.utils.data import Dataset
from tqdm import tqdm
import pandas as pd
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetQuery
from mimic.utils.db import DuckDB
from .downloadable import Downloadable


class MIMIC_IV(Dataset, Downloadable):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        sheets: dict[str, Sheet],
        column_id: str,
        columns: list[str],
        download: bool = False,
    ):
        env = Env()

        super().__init__(root=root, credentials=env.credentials)

        self.root = root
        self.db = db
        self.sheets = sheets
        self.column_id = column_id
        self.columns = columns

        self.resources = []
        for f in env.iv_files:
            if f["name"] in list(sheets.keys()):
                self.resources.append(f)

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

        self._load_data()

        self.main_query = self._calc_query(only_count=False)
        self.count_query = self._calc_query(only_count=True)

    def _get_resources(self):
        return self.resources

    def _load_data(self):
        for k in tqdm(self.sheets, desc="Loading data"):
            self.sheets[k].load_csv()

    def _calc_query(self, only_count: bool = False) -> SheetQuery:
        query = SheetQuery.empty()
        sheets = list(self.sheets.values())

        if only_count:
            query = SheetQuery.count(sheets[0])
        else:
            query = SheetQuery.select(sheets[0], self.columns)

        prev_sheet = sheets[0]

        for sheet in sheets[1:]:
            query.join(prev_sheet, sheet)

        return query

    def get_by_id(self, id: list[str]) -> pd.DataFrame:
        query = self.main_query.find_by_id(self.column_id, id, inplace=False)

        df = self.db.fetch_df(query).drop(columns=["row_num"])

        return df

    def __len__(self) -> int:
        count = self.db.fetch_one(self.count_query)[0]

        return count

    def __getitem__(self, idx: int):
        return idx

    def collate_fn(self, idx: list[int]):
        query = self.main_query.find_by_row_id(idx, inplace=False)

        df = self.db.fetch_df(query).drop(columns=["row_num"])

        df_tensor = torch.from_numpy(df.to_numpy())

        return df_tensor
