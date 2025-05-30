import os
from pathlib import Path
from typing import Union
import torch
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity
from tqdm import tqdm
import pandas as pd
from utils import download_and_extract_archive
from utils.env import Env
from utils.sheet import Sheet, SheetQuery
from utils.db import DuckDB


class MIMIC_IV(Dataset):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        sheets: dict[str, Sheet],
        column_id: str,
        columns: list[str],
        download: bool = False,
    ):
        super().__init__()

        env = Env()

        self.root = root
        self.db = db
        self.credentials = env.credentials
        self.sheets = sheets
        self.column_id = column_id
        self.columns = columns

        self.resources = []
        for f in env.iv_files:
            if f["name"] in list(sheets.keys()):
                self.resources.append(f)

        if download:
            self.download()

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

        self._load_data()

        self.main_query = self._calc_query(only_count=False)
        self.count_query = self._calc_query(only_count=True)

    def _check_exists(self) -> bool:
        return all(
            check_integrity(
                os.path.join(
                    self.raw_folder,
                    os.path.splitext(os.path.basename(f["url"]))[0],
                )
            )
            for f in self.resources
        )

    @property
    def raw_folder(self) -> str:
        return os.path.join(self.root, self.__class__.__name__)

    @staticmethod
    def get_raw_folder(root: str):
        return os.path.join(root, MIMIC_IV.__name__)

    def download(self):
        if self._check_exists():
            return

        os.makedirs(self.raw_folder, exist_ok=True)

        for f in self.resources:
            download_and_extract_archive(
                url=f["url"],
                download_root=self.raw_folder,
                credentials=self.credentials,
                md5=f["md5"],
            )

    def _load_data(self):
        self.data = {}

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

    def get_by_id(self, id: int) -> pd.DataFrame:
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
