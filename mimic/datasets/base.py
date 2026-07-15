from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import torch
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity
from tqdm import tqdm

from mimic.utils import download_and_extract_archive
from mimic.utils.db import DuckDB
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetJoinCondition, SheetQuery


class BaseDataset(Dataset, ABC):
    def __init__(
        self,
        root: str | Path,
        db: DuckDB,
        column_id: str,
        columns: str | list[str],
        sheets: dict[str, Sheet],
        join_conditions: list[SheetJoinCondition],
        *,
        download: bool = False,
        skip_load: bool = False,
    ) -> None:
        super().__init__()

        env = Env()

        self.root = Path(root)
        self.db = db
        self.column_id = column_id
        self.columns = columns
        self.sheets = sheets
        self.join_conditions = join_conditions
        self.credentials = env.credentials

        self.resources = []
        for k in self.sheets:
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
            msg = "Dataset not found. You can use download=True to download it"
            raise RuntimeError(msg)

        self._load_data()

    @abstractmethod
    def _files(self) -> dict[str, dict[str, Any]]:
        pass

    def _check_exists(self) -> bool:
        return all(
            check_integrity(Path(f.get("download_root")) / Path(f.get("url")).stem)
            for f in self.resources
        )

    @property
    def raw_folder(self) -> Path:
        return self.root / self.__class__.__name__

    @classmethod
    def get_raw_folder(cls, root: Path) -> Path:
        return root / cls.__name__

    def _download(self) -> None:
        if self._check_exists():
            return

        self.raw_folder.mkdir(parents=True, exist_ok=True)

        for f in self.resources:
            download_and_extract_archive(
                url=f.get("url"),
                download_root=f.get("download_root"),
                credentials=self.credentials,
                md5=f.get("md5"),
            )

    def _load_data(self) -> None:
        for k in tqdm(self.sheets, desc="Loading data"):
            self.sheets[k].load_csv()

    def _calc_query(
        self,
        columns: list[str] | str | None = None,
        *,
        only_count: bool = False,
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

    def get_by_id(self, ids: list[str]) -> pd.DataFrame:
        query = self.main_query.find_by_id(self.column_id, ids, inplace=False)

        return self.db.fetch_df(query).drop(columns=["row_num"])

    def __len__(self) -> int:
        res = self.db.fetch_one(self.count_query)
        if res is None:
            return 0
        return res[0]

    def __getitem__(self, idx: int) -> int:
        return idx

    @abstractmethod
    def collate_fn(self, idx: list[int]) -> Sequence[torch.Tensor] | torch.Tensor:
        pass
