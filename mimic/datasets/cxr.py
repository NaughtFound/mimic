from pathlib import Path
from typing import Callable, Literal, Union
import torch
from torch.utils.data import Dataset
from tqdm import tqdm
import pandas as pd
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetQuery
from mimic.utils.db import DuckDB
from .downloadable import Downloadable


def transform_split(db: DuckDB, df: pd.DataFrame) -> pd.DataFrame:
    return df


class MIMIC_CXR(Dataset, Downloadable):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        study: Literal["chexpert", "negbio"] = "chexpert",
        study_transform: Callable[[DuckDB, pd.DataFrame], pd.DataFrame] = None,
        study_table_fields: dict[str, str] = None,
        transform: Callable = None,
        download: bool = False,
        **kwargs,
    ):
        env = Env()

        super().__init__(root=root, credentials=env.credentials)

        self.root = root
        self.db = db
        self.column_id = "study_id"
        self.transform = transform
        self.study = study
        self.study_transform = study_transform
        self.study_table_fields = study_table_fields
        self.kwargs = kwargs

        self.resources = []
        for f in env.cxr_files:
            if f["name"] in ["split", self.study]:
                self.resources.append(f)

        self.sheets = self._create_sheets()

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

        self._load_data()

        self.main_query = self._calc_query(only_count=False)
        self.count_query = self._calc_query(only_count=True)

    def _create_sheets(self) -> dict[str, Sheet]:
        split_sheet = Sheet(
            root=self.root,
            db=self.db,
            columns={
                "dicom_id": "string",
                "study_id": "string",
                "subject_id": "int",
                "split": "string",
            },
            table_fields={
                "image_path": "string",
                "study_id": "string",
                "subject_id": "int",
                "split": "string",
            },
            id_column="dicom_id",
            table_name="split",
            transform=transform_split,
            **self.kwargs,
        )

        study_sheet = Sheet(
            root=self.root,
            db=self.db,
            columns={
                "subject_id": "string",
                "study_id": "string",
                "Atelectasis": "int",
                "Cardiomegaly": "int",
                "Consolidation": "int",
                "Edema": "int",
                "Enlarged Cardiomediastinum": "int",
                "Fracture": "int",
                "Lung Lesion": "int",
                "Lung Opacity": "int",
                "Pleural Effusion": "int",
                "Pneumonia": "int",
                "Pneumothorax": "int",
                "Pleural Other": "int",
                "Support Devices": "int",
                "No Finding": "int",
            },
            table_fields=self.study_table_fields,
            id_column="study_id",
            table_name="study",
            transform=self.study_transform,
            **self.kwargs,
        )

        return {
            "split": split_sheet,
            self.study: study_sheet,
        }

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
