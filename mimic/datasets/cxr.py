from pathlib import Path
from typing import Any, Callable, Literal, Union
import torch
import pandas as pd
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet
from mimic.utils.db import DuckDB
from .base import BaseDataset


def transform_split(db: DuckDB, df: pd.DataFrame) -> pd.DataFrame:
    df["image_path"] = (
        "p"
        + df["subject_id"].str[:2]
        + "/p"
        + df["subject_id"]
        + "/s"
        + df["study_id"]
        + "/"
        + df["dicom_id"]
        + ".jpg"
    )

    return df


class MIMIC_CXR(BaseDataset):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        columns: Union[str, list[str]],
        study: Literal["chexpert", "negbio"] = "chexpert",
        study_transform: Callable[[DuckDB, pd.DataFrame], pd.DataFrame] = None,
        study_table_fields: dict[str, str] = None,
        transform: Callable = None,
        download: bool = False,
        mode: Literal["train", "test", "validate"] = "train",
        **kwargs,
    ):
        env = Env()

        self.root = root
        self.db = db
        self.column_id = "study_id"
        self.columns = columns
        self.study = study
        self.study_transform = study_transform
        self.study_table_fields = study_table_fields
        self.transform = transform
        self.kwargs = kwargs
        self.sheets = self._create_sheets(env.cxr_files)

        super().__init__(
            root=self.root,
            db=self.db,
            column_id=self.column_id,
            columns=self.columns,
            sheets=self.sheets,
            download=download,
        )

        split_condition = f"split='{mode}'"

        self.main_query.where(split_condition, inplace=True)
        self.count_query.where(split_condition, inplace=True)

    def _files(self):
        return Env().cxr_files

    def _create_sheets(self, cxr_files: dict[str, Any]) -> dict[str, Sheet]:
        split_sheet = Sheet(
            root=self.raw_folder,
            db=self.db,
            columns={
                "dicom_id": "string",
                "study_id": "string",
                "subject_id": "string",
                "split": "string",
            },
            table_fields={
                "image_path": "string",
                "dicom_id": "string",
                "study_id": "string",
                "subject_id": "string",
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
                "Atelectasis": "float",
                "Cardiomegaly": "float",
                "Consolidation": "float",
                "Edema": "float",
                "Enlarged Cardiomediastinum": "float",
                "Fracture": "float",
                "Lung Lesion": "float",
                "Lung Opacity": "float",
                "Pleural Effusion": "float",
                "Pneumonia": "float",
                "Pneumothorax": "float",
                "Pleural Other": "float",
                "Support Devices": "float",
                "No Finding": "float",
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

    def collate_fn(self, idx: list[int]):
        query = self.main_query.find_by_row_id(idx, inplace=False)

        df = self.db.fetch_df(query).drop(columns=["row_num"])

        df_tensor = torch.from_numpy(df.to_numpy())

        return df_tensor
