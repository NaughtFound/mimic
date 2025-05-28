import os
from pathlib import Path
from typing import Callable, Union
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity
import duckdb
import pandas as pd
from tqdm import tqdm
from utils import download_and_extract_archive
from utils.env import Env
from utils.scaler import Scaler


class MIMIC_IV_Sheet:
    def __init__(
        self,
        root: str,
        db_path: str,
        table_name: str,
        columns: dict[str, str],
        id_column: str,
        scaler: list[Scaler] = None,
        transform: Callable[[pd.DataFrame], pd.DataFrame] = None,
        train: bool = True,
        clear_before_insert: bool = True,
        force_insert: bool = False,
    ):
        self.root = root
        self.table_name = table_name
        self.columns = columns
        self.id_column = id_column
        self.scaler = scaler
        self.transform = transform
        self.train = train
        self.force_insert = force_insert

        os.makedirs(MIMIC_IV.get_raw_folder(root), exist_ok=True)

        self.source_csv_path = os.path.join(
            MIMIC_IV.get_raw_folder(root),
            f"{table_name}.csv",
        )
        self.db_path = db_path

        self.connection = duckdb.connect(database=self.db_path, read_only=False)

        self._create_table()

        if clear_before_insert:
            self._clear_table()

    def _create_table(self):
        columns_str = ", ".join(f"{col} {dtype}" for col, dtype in self.columns.items())
        create_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_str});"
        self.connection.execute(create_query)

    def _clear_table(self):
        delete_query = f"DELETE FROM {self.table_name};"
        self.connection.execute(delete_query)

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.transform is not None:
            df = self.transform(df)

        if self.scaler is None:
            return df

        for s in self.scaler:
            df = s.transform(df)

        return df

    def _insert_data(self, csv_path: str):
        insert_query = (
            f"COPY {self.table_name} FROM '{csv_path}' (FORMAT CSV, HEADER TRUE);"
        )
        self.connection.execute(insert_query)

    def load_csv(self):
        csv_path = os.path.join(self.root, "transformed", f"{self.table_name}.csv")

        if os.path.exists(csv_path) and not self.force_insert:
            return

        df = pd.read_csv(
            self.source_csv_path,
            usecols=self.columns.keys(),
            dtype=self.columns,
        )

        if self.id_column not in df.columns:
            raise ValueError(f"CSV file must contain an '{self.id_column}' column.")

        df = self._transform_data(df)

        os.makedirs(os.path.join(self.root, "transformed"), exist_ok=True)

        df.to_csv(csv_path, index=False)

        self._insert_data(csv_path)

    def close(self):
        self.connection.close()


class MIMIC_IV(Dataset):
    def __init__(
        self,
        root: Union[str, Path],
        sheets: dict[str, MIMIC_IV_Sheet],
        download: bool = False,
    ):
        super().__init__()

        env = Env()

        self.root = root
        self.credentials = env.credentials
        self.sheets = sheets

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
