import base64
import os
import pathlib
import urllib
from typing import Optional, Union
from torchvision.datasets.utils import check_integrity, extract_archive
from tqdm import tqdm

USER_AGENT = "physionet/mimic"


def _urlretrieve(
    url: str,
    filename: Union[str, pathlib.Path],
    credentials: dict[str, str],
    chunk_size: int = 1024 * 32,
) -> None:
    username = credentials["username"]
    password = credentials["password"]
    auth_string = f"{username}:{password}"
    auth_header = base64.b64encode(auth_string.encode()).decode()

    with urllib.request.urlopen(
        urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Basic {auth_header}",
            },
        )
    ) as response:
        with (
            open(filename, "wb") as fh,
            tqdm(total=response.length, unit="B", unit_scale=True) as pbar,
        ):
            while chunk := response.read(chunk_size):
                fh.write(chunk)
                pbar.update(len(chunk))


def download_url(
    url: str,
    root: Union[str, pathlib.Path],
    credentials: dict[str, str],
    filename: Optional[Union[str, pathlib.Path]] = None,
    md5: Optional[str] = None,
) -> None:
    root = os.path.expanduser(root)
    if not filename:
        filename = os.path.basename(url)
    fpath = os.fspath(os.path.join(root, filename))

    os.makedirs(root, exist_ok=True)

    if check_integrity(fpath, md5):
        print("Using downloaded and verified file: " + fpath)
        return

    print("Downloading " + url + " to " + fpath)
    _urlretrieve(url, fpath, credentials)

    if not check_integrity(fpath, md5):
        raise RuntimeError("File not found or corrupted.")


def download_and_extract_archive(
    url: str,
    download_root: Union[str, pathlib.Path],
    credentials: dict[str, str],
    extract_root: Optional[Union[str, pathlib.Path]] = None,
    filename: Optional[Union[str, pathlib.Path]] = None,
    md5: Optional[str] = None,
    remove_finished: bool = False,
) -> None:
    download_root = os.path.expanduser(download_root)
    if extract_root is None:
        extract_root = download_root
    if not filename:
        filename = os.path.basename(url)

    download_url(
        url=url,
        root=download_root,
        credentials=credentials,
        filename=filename,
        md5=md5,
    )

    archive = os.path.join(download_root, filename)
    print(f"Extracting {archive} to {extract_root}")
    extract_archive(archive, extract_root, remove_finished)
