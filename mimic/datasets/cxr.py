import os
from pathlib import Path
from typing import Any, Callable, Literal, Union
import torch
import pandas as pd
from tqdm import tqdm
from PIL import Image
from torchvision import transforms
from mimic.utils import download_url, check_integrity
from mimic.utils.env import Env
from mimic.utils.sheet import Sheet, SheetQuery, SheetJoinCondition
from mimic.utils.db import DuckDB
from .base import BaseDataset


def _transform_split(db: DuckDB, df: pd.DataFrame) -> pd.DataFrame:
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
        download_condition: Callable[[SheetQuery, str], SheetQuery] = None,
        skip_load: bool = False,
        **kwargs,
    ):
        env = Env()

        self.root = root
        self.db = db
        self.column_id = "study_id"
        self.columns = self._ensure_columns(columns, ["split.dicom_id", "image_path"])
        self.label_proportions = label_proportions
        self.study = study
        self.study_transform = study_transform
        self.study_table_fields = study_table_fields
        self.transform = transform or self._create_transform()
        self.use_metadata = use_metadata
        self.metadata_transform = metadata_transform
        self.metadata_table_fields = metadata_table_fields
        self.download_condition = download_condition
        self.kwargs = kwargs
        self.sheets = self._create_sheets(env.cxr_files)

        super().__init__(
            root=self.root,
            db=self.db,
            column_id=self.column_id,
            columns=self.columns,
            sheets=self.sheets,
            join_conditions=self._create_join_conditions(),
            download=download,
            skip_load=skip_load,
        )

        split_condition = f"split='{mode}'"

        self.main_query.where(split_condition, inplace=True)
        self.count_query.where(split_condition, inplace=True)

        if download:
            self._download_images()

        if skip_load:
            return

        if not self._check_images_exists():
            raise RuntimeError(
                "Images not found. You can use download=True to download them"
            )

    def _create_transform(self):
        return transforms.Compose(
            [
                transforms.Resize((1500, 1500)),
                transforms.ToTensor(),
            ]
        )

    def _create_join_conditions(self) -> list[SheetJoinCondition]:
        join_conditions = [
            SheetJoinCondition(
                l_sheet=self.sheets["split"],
                r_sheet=self.sheets[self.study],
                columns=("study_id", "study_id"),
                mode="left",
            )
        ]

        if self.use_metadata:
            join_conditions.append(
                SheetJoinCondition(
                    l_sheet=self.sheets["split"],
                    r_sheet=self.sheets["metadata"],
                    columns=("dicom_id", "dicom_id"),
                    mode="left",
                )
            )

        return join_conditions

    def _ensure_columns(
        self,
        columns: Union[str, list[str]],
        required_columns: list[str],
    ) -> Union[str, list[str]]:
        if columns != "*":
            if isinstance(columns, list):
                columns.extend(col for col in required_columns if col not in columns)
            else:
                for col in required_columns:
                    if col not in columns:
                        columns += f",{col}"
        return columns

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

        main_query = self._calc_query(
            only_count=False,
            downloaded_only=False,
            columns="split.dicom_id",
        )

        count_query = self._calc_query(
            only_count=True,
            downloaded_only=False,
        )

        for k in self.label_proportions:
            m_query = main_query.copy()
            c_query = count_query.copy()

            if self.download_condition is not None:
                m_query = self.download_condition(m_query, k)
                c_query = self.download_condition(c_query, k)
            else:
                condition = f"{SheetQuery._parse_column(k)} IS NOT NULL"
                m_query.where(condition)
                c_query.where(condition)

            total = self.db.fetch_one(c_query)[0]
            total_download = int(total * self.label_proportions[k])

            m_query.limit(total_download)

            rows = self.db.fetch_df(m_query)["dicom_id"].to_list()

            if len(rows) == 0:
                continue

            self.db.exec(
                SheetQuery.update(
                    self.sheets["split"],
                    {"download": True},
                ).find_by_id("dicom_id", rows)
            )

        files = self.db.fetch_df(self.main_query)["image_path"].to_list()

        for file in tqdm(files, desc="Downloading Images"):
            file_url = f"{env.cxr_url}/{file}"
            file_root = os.path.dirname(file)
            file_path = os.path.join(self.raw_folder, file_root)

            if os.path.exists(os.path.join(self.raw_folder, file)):
                continue

            download_url(
                url=file_url,
                root=file_path,
                credentials=env.credentials,
                verbose=False,
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
            transform=_transform_split,
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
                    "ViewPosition": "string",
                    "Rows": "string",
                    "Columns": "string",
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
        df = self.db.fetch_df(query).drop(columns=["row_num", "dicom_id"])

        images = [
            self._load_image(os.path.join(self.raw_folder, img_path))
            for img_path in df["image_path"]
        ]

        images = [self.transform(img) for img in images]

        df_tensor = torch.from_numpy(df.drop(columns=["image_path"]).to_numpy())
        image_tensors = torch.stack(images)

        return image_tensors, df_tensor
