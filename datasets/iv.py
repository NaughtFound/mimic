import os
from pathlib import Path
from typing import Union
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity
from tqdm import tqdm
from utils import download_and_extract_archive
from utils.env import Env
from utils.sheet import Sheet


class MIMIC_IV(Dataset):
    def __init__(
        self,
        root: Union[str, Path],
        sheets: dict[str, Sheet],
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
