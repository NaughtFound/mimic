import torch
from mimic.utils.env import Env
from .base import BaseDataset


class MIMIC_IV(BaseDataset):
    def _files(self):
        return Env().iv_files

    def collate_fn(self, idx: list[int]):
        query = self.main_query.find_by_row_id(idx, inplace=False)

        df = self.db.fetch_df(query).drop(columns=["row_num"])

        df = self.transform_df(df)

        df_tensor = torch.from_numpy(df.to_numpy())

        return df_tensor
