import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Sequence, Union
import pandas as pd
from tqdm import tqdm
import torch
from torchvision.datasets.utils import check_integrity
from torch.utils.data import Dataset
from mimic.utils import download_and_extract_archive
from mimic.utils.db import DuckDB
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetQuery, SheetJoinCondition


class BaseDataset(Dataset, ABC):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        column_id: str,
        columns: Union[str, list[str]],
        sheets: dict[str, Sheet],
        join_conditions: list[SheetJoinCondition],
        download: bool = False,
        skip_load: bool = False,
    ):
        super().__init__()

        env = Env()

        self.root = root
        self.db = db
        self.column_id = column_id
        self.columns = columns
        self.sheets = sheets
        self.join_conditions = join_conditions
        self.credentials = env.credentials

        self.resources = []
        for k in self.sheets.keys():
            resource = self._files()[k]
            resource["download_root"] = self.sheets[k].root
            self.resources.append(resource)

        self.main_query = self._calc_query(only_count=False)
        self.count_query = self._calc_query(only_count=True)

        if download:
            self._download()

        if skip_load:
            return

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

        self._load_data()

    @abstractmethod
    def _files(self) -> dict[str, dict[str, Any]]:
        pass

    def _check_exists(self) -> bool:
        return all(
            check_integrity(
                os.path.join(
                    f.get("download_root"),
                    os.path.splitext(os.path.basename(f.get("url")))[0],
                )
            )
            for f in self.resources
        )

    @property
    def raw_folder(self) -> str:
        return os.path.join(self.root, self.__class__.__name__)

    @classmethod
    def get_raw_folder(cls, root: str):
        return os.path.join(root, cls.__name__)

    def _download(self):
        if self._check_exists():
            return

        os.makedirs(self.raw_folder, exist_ok=True)

        for f in self.resources:
            download_and_extract_archive(
                url=f.get("url"),
                download_root=f.get("download_root"),
                credentials=self.credentials,
                md5=f.get("md5"),
            )

    def _load_data(self):
        for k in tqdm(self.sheets, desc="Loading data"):
            self.sheets[k].load_csv()

    def _calc_query(
        self,
        only_count: bool = False,
        columns: list[str] = None,
    ) -> SheetQuery:
        query = SheetQuery.empty()
        first_sheet = self.join_conditions[0].l_sheet

        if only_count:
            query = SheetQuery.count(first_sheet, columns=columns or "*")
        else:
            query = SheetQuery.select(first_sheet, columns=columns or self.columns)

        for condition in self.join_conditions:
            query.join(condition)

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

    @abstractmethod
    def collate_fn(self, idx: list[int]) -> Sequence[torch.Tensor]:
        pass
