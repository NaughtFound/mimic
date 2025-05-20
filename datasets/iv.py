import os
from pathlib import Path
from typing import Union
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity
from utils import download_and_extract_archive
from utils.env import Env


class MIMIC_IV(Dataset):
    def __init__(
        self,
        root: Union[str, Path],
        download: bool = False,
        download_whitelist: list[str] = None,
    ):
        super().__init__()

        env = Env()

        self.root = root
        self.credentials = env.credentials

        if download_whitelist is not None:
            self.resources = []
            for f in env.iv_files:
                if f["name"] in download_whitelist:
                    self.resources.append(f)
        else:
            self.resources = env.iv_files

        if download:
            self.download()

        if not self._check_exists():
            raise RuntimeError(
                "Dataset not found. You can use download=True to download it"
            )

    def _check_exists(self) -> bool:
        return all(
            check_integrity(
                os.path.join(
                    self.raw_folder, os.path.splitext(os.path.basename(f["url"]))[0]
                )
            )
            for f in self.resources
        )

    @property
    def raw_folder(self) -> str:
        return os.path.join(self.root, self.__class__.__name__)

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
