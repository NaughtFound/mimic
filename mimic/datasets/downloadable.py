import os
from abc import ABC, abstractmethod
from mimic.utils import download_and_extract_archive
from torchvision.datasets.utils import check_integrity


class Downloadable(ABC):
    def __init__(self, root: str, credentials: dict[str, str]):
        super().__init__()

        self.root = root
        self.credentials = credentials

    @abstractmethod
    def _get_resources(self) -> list[dict]:
        pass

    def _check_exists(self) -> bool:
        return all(
            check_integrity(
                os.path.join(
                    self.raw_folder,
                    os.path.splitext(os.path.basename(f.get("url")))[0],
                )
            )
            for f in self._get_resources()
        )

    @property
    def raw_folder(self) -> str:
        return os.path.join(self.root, self.__class__.__name__)

    @classmethod
    def get_raw_folder(cls, root: str):
        return os.path.join(root, cls.__name__)

    def _download(self):
        if self._check_exists():
            return

        os.makedirs(self.raw_folder, exist_ok=True)

        for f in self._get_resources():
            download_and_extract_archive(
                url=f.get("url"),
                download_root=self.raw_folder,
                credentials=self.credentials,
                md5=f.get("md5"),
            )
