from typing import Any

import torch

from mimic.utils.env import Env

from .base import BaseDataset


class IV(BaseDataset):
    def _files(self) -> dict[str, dict[str, Any]]:
        return Env().iv_files

    def collate_fn(self, idx: list[int]) -> torch.Tensor:
        query = self.main_query.find_by_row_id(idx, inplace=False)

        df = self.db.fetch_df(query).drop(columns=["row_num"])

        return torch.from_numpy(df.to_numpy())
