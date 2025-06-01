from pathlib import Path
from typing import Any, Callable, Literal, Union
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

        self.sheets = self._create_sheets(env.cxr_files)

        self.resources = []
        for k in self.sheets.keys():
            self.resources.append(env.cxr_files[k])

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

        self._load_data()

    def _create_sheets(self, cxr_files: dict[str, Any]) -> dict[str, Sheet]:
        split_sheet = Sheet(
            root=self.raw_folder,
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
            file_name=cxr_files["split"]["name"],
            transform=transform_split,
            **self.kwargs,
        )

        study_sheet = Sheet(
            root=self.raw_folder,
            db=self.db,
            columns={
                "subject_id": "string",
                "study_id": "string",
                "Atelectasis": "string",
                "Cardiomegaly": "string",
                "Consolidation": "string",
                "Edema": "string",
                "Enlarged Cardiomediastinum": "string",
                "Fracture": "string",
                "Lung Lesion": "string",
                "Lung Opacity": "string",
                "Pleural Effusion": "string",
                "Pneumonia": "string",
                "Pneumothorax": "string",
                "Pleural Other": "string",
                "Support Devices": "string",
                "No Finding": "string",
            },
            table_fields=self.study_table_fields,
            id_column="study_id",
            table_name="study",
            file_name=cxr_files[self.study]["name"],
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
