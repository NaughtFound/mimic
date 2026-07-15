import base64
import logging
import os
from pathlib import Path

import requests
from torchvision.datasets.utils import check_integrity, extract_archive
from tqdm import tqdm


def _urlretrieve(
    url: str,
    filename: str | Path,
    credentials: dict[str, str],
    chunk_size: int = 1024 * 32,
) -> None:
    filename = Path(filename)
    username = credentials["username"]
    password = credentials["password"]
    auth_string = f"{username}:{password}"
    auth_header = base64.b64encode(auth_string.encode()).decode()
    headers = {
        "User-Agent": "Wget/1.21.4",
        "Authorization": f"Basic {auth_header}",
    }

    try:
        with requests.get(
            url,
            headers=headers,
            stream=True,
            timeout=(5, 30),
        ) as response:
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            with (
                filename.open("wb") as fh,
                tqdm(total=total_size, unit="B", unit_scale=True) as p_bar,
            ):
                for chunk in response.iter_content(chunk_size=chunk_size):
                    fh.write(chunk)
                    p_bar.update(len(chunk))

    except requests.exceptions.Timeout:
        logging.info("The request timed out!")
    except requests.exceptions.RequestException as e:
        logging.info(f"An error occurred: {e}")


def download_url(
    url: str,
    root: str | Path,
    credentials: dict[str, str],
    filename: str | Path | None = None,
    md5: str | None = None,
    *,
    verbose: bool = True,
) -> None:
    root = Path(root).expanduser()
    if not filename:
        filename = Path(url).name

    fpath = os.fspath(root / filename)

    root.mkdir(parents=True, exist_ok=True)

    if check_integrity(fpath, md5):
        if verbose:
            logging.info(f"Using downloaded and verified file: {fpath}")
        return

    if verbose:
        logging.info(f"Downloading {url} to {fpath}")
    _urlretrieve(url, fpath, credentials)

    if not check_integrity(fpath, md5):
        msg = "File not found or corrupted."
        raise RuntimeError(msg)


def download_and_extract_archive(
    url: str,
    download_root: str | Path,
    credentials: dict[str, str],
    extract_root: str | Path | None = None,
    filename: str | Path | None = None,
    md5: str | None = None,
    *,
    remove_finished: bool = False,
    verbose: bool = True,
) -> None:
    download_root = Path(download_root).expanduser()
    if extract_root is None:
        extract_root = download_root
    if not filename:
        filename = Path(url).name

    download_url(
        url=url,
        root=download_root,
        credentials=credentials,
        filename=filename,
        md5=md5,
        verbose=verbose,
    )

    archive = download_root / filename
    if verbose:
        logging.info(f"Extracting {archive} to {extract_root}")
    extract_archive(archive, extract_root, remove_finished)
