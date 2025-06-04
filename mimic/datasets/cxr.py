import os
from pathlib import Path
from typing import Any, Callable, Literal, Union
import torch
import pandas as pd
from PIL import Image
from mimic.utils import download_url, check_integrity
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetQuery, SheetJoinCondition
from mimic.utils.db import DuckDB
from .base import BaseDataset


def transform_split(db: DuckDB, df: pd.DataFrame) -> pd.DataFrame:
    df["image_path"] = (
        "files/p"
        + df["subject_id"].str[:2]
        + "/p"
        + df["subject_id"]
        + "/s"
        + df["study_id"]
        + "/"
        + df["dicom_id"]
        + ".jpg"
    )

    df["download"] = False

    return df


class MIMIC_CXR(BaseDataset):
    def __init__(
        self,
        root: Union[str, Path],
        db: DuckDB,
        columns: Union[str, list[str]],
        label_proportions: dict[str, float],
        study: Literal["chexpert", "negbio"] = "chexpert",
        study_transform: Callable[[DuckDB, pd.DataFrame], pd.DataFrame] = None,
        study_table_fields: dict[str, str] = None,
        transform: Callable = None,
        download: bool = False,
        mode: Literal["train", "test", "validate"] = "train",
        use_metadata: bool = False,
        metadata_transform: Callable[[DuckDB, pd.DataFrame], pd.DataFrame] = None,
        metadata_table_fields: dict[str, str] = None,
        **kwargs,
    ):
        env = Env()

        self.root = root
        self.db = db
        self.column_id = "study_id"
        self.columns = columns
        self.label_proportions = label_proportions
        self.study = study
        self.study_transform = study_transform
        self.study_table_fields = study_table_fields
        self.transform = transform
        self.use_metadata = use_metadata
        self.metadata_transform = metadata_transform
        self.metadata_table_fields = metadata_table_fields
        self.kwargs = kwargs
        self.sheets = self._create_sheets(env.cxr_files)

        join_conditions = [
            SheetJoinCondition(
                l_sheet=self.sheets["split"],
                r_sheet=self.sheets[self.study],
                columns=("study_id", "study_id"),
                mode="left",
            )
        ]

        if use_metadata:
            join_conditions.append(
                SheetJoinCondition(
                    l_sheet=self.sheets["split"],
                    r_sheet=self.sheets["metadata"],
                    columns=("dicom_id", "dicom_id"),
                    mode="left",
                )
            )

        super().__init__(
            root=self.root,
            db=self.db,
            column_id=self.column_id,
            columns=self.columns,
            sheets=self.sheets,
            join_conditions=join_conditions,
            download=download,
        )

        split_condition = f"split='{mode}'"

        self.main_query.where(split_condition, inplace=True)
        self.count_query.where(split_condition, inplace=True)

        if download:
            self._download_images()

        if not self._check_images_exists():
            raise RuntimeError(
                "Images not found. You can use download=True to download them"
            )

    def _check_images_exists(self):
        files = self.db.fetch_df(self.main_query)["image_path"].to_list()

        if len(files) == 0:
            return False

        return all(check_integrity(os.path.join(self.raw_folder, f)) for f in files)

    def _calc_query(
        self,
        only_count: bool = False,
        downloaded_only: bool = True,
        columns: list[str] = None,
    ):
        query = super()._calc_query(only_count, columns)

        if downloaded_only:
            download_condition = "download=True"
            query.where(download_condition, inplace=True)

        return query

    def _download_images(self):
        env = Env()

        count_query = self._calc_query(
            only_count=True,
            downloaded_only=False,
            columns=self.label_proportions.keys(),
        )
        download_query = self._calc_query(
            only_count=False,
            downloaded_only=False,
        )

        total = self.db.fetch_df(count_query)
        proportion = pd.Series(self.label_proportions)
        total_download = total.mul(proportion, axis=1).iloc[0].to_dict()

        for k in total_download:
            query = download_query.where(
                f'"{k}" IS NOT NULL',
                inplace=False,
            ).limit(
                int(total_download[k]),
                inplace=False,
            )
            rows = self.db.fetch_df(query)["dicom_id"].to_list()

            if len(rows) == 0:
                continue

            self.db.exec(
                SheetQuery.update(
                    self.sheets["split"],
                    {"download": True},
                ).find_by_id("dicom_id", rows)
            )

        files = self.db.fetch_df(self.main_query)["image_path"].to_list()

        for file in files:
            file_url = f"{env.cxr_url}/{file}"
            file_root = os.path.dirname(file)
            file_path = os.path.join(self.raw_folder, file_root)

            if os.path.exists(os.path.join(self.raw_folder, file)):
                continue

            download_url(
                url=file_url,
                root=file_path,
                credentials=env.credentials,
            )

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
                "download": "boolean",
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

        sheets = {
            "split": split_sheet,
            self.study: study_sheet,
        }

        if self.use_metadata:
            metadata_sheet = Sheet(
                root=self.raw_folder,
                db=self.db,
                columns={
                    "dicom_id": "string",
                    "subject_id": "string",
                    "study_id": "string",
                    "PerformedProcedureStepDescription": "string",
                    "ViewPosition": "string",
                    "Rows": "int",
                    "Columns": "int",
                    "StudyDate": "string",
                    "StudyTime": "string",
                    "ProcedureCodeSequence_CodeMeaning": "string",
                    "ViewCodeSequence_CodeMeaning": "string",
                    "PatientOrientationCodeSequence_CodeMeaning": "string",
                },
                table_fields=self.metadata_table_fields,
                id_column="dicom_id",
                table_name="metadata",
                file_name=cxr_files["metadata"]["name"],
                transform=self.metadata_transform,
                **self.kwargs,
            )

            sheets["metadata"] = metadata_sheet

        return sheets

    def _load_image(self, img_path: str):
        image = Image.open(img_path)

        if image.mode not in ["RGB", "L"]:
            image = image.convert("RGB")

        return image

    def collate_fn(self, idx: list[int]):
        query = self.main_query.find_by_row_id(idx, inplace=False)
        df = self.db.fetch_df(query).drop(columns=["row_num", "download"])

        image_paths = os.path.join(self.raw_folder, "files/") + df["image_path"]

        images = [self._load_image(img_path) for img_path in image_paths]

        if self.transform is not None:
            images = [self.transform(img) for img in images]

        df_tensor = torch.from_numpy(df.drop(columns=["image_path"]).to_numpy())
        image_tensors = torch.stack([torch.tensor(img) for img in images])

        return image_tensors, df_tensor
