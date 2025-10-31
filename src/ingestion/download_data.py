"""Functions for data ingestion process."""

import logging
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

from utils.logger import setup_logger

# Setup logger for this module
logger = setup_logger(__name__)


def download_file(
    url: str, local_path: str, chunk_size: int = 8192, show_progress: bool = True
) -> bool:
    """
    Download file from a web URL and store it at the given local_path location.

    Args:
        url: URL of the file to be downloaded
        local_path: Path where the file will be stored
        chunk_size: Size of chunks to download (in bytes)
        show_progress: Whether to show download progress bar

    Returns:
        Boolean indicating if the operation was successful
    """
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.debug(f"Starting download from {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        logger.debug(f"File size: {total_size / (1024 * 1024):.2f} MB")

        with open(local_path, "wb") as file:
            if show_progress and total_size > 0:
                with tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=local_path.name
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)

        logger.info(f"Successfully downloaded: {local_path}")
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error downloading {url}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error downloading {url}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        return False
    except IOError as e:
        logger.error(f"Error writing file {local_path}: {e}")
        return False


def download_insee_files(
    config: dict,
    output_dir: Optional[str] = None,
    files_to_download: Optional[list] = None,
) -> dict:
    """
    Download INSEE SIRENE files based on configuration.

    Args:
        config: Configuration dictionary with INSEE URLs
        output_dir: Optional custom output directory (defaults to bronze path)
        files_to_download: Optional list of specific files to download
                          (e.g., ['stock_etablissement', 'stock_unitelegale'])
                          If None, downloads all available files

    Returns:
        Dictionary with download results {filename: success_bool}
    """
    from datetime import datetime

    from utils.config import get_data_paths, get_insee_urls

    insee_urls = get_insee_urls(config)
    data_paths = get_data_paths(config)

    if output_dir is None:
        timestamp = datetime.now().strftime("%Y-%m")
        output_dir = Path(data_paths["bronze"]) / timestamp
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting INSEE data download to: {output_dir}")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Filter URLs if specific files requested
    urls_to_download = {}
    for key, url in insee_urls.items():
        if key.endswith("_url"):
            file_type = key.replace("_url", "")
            if files_to_download is None or file_type in files_to_download:
                urls_to_download[file_type] = url

    logger.info(f"Files to download: {list(urls_to_download.keys())}")

    # Download each file
    results = {}
    for file_type, url in urls_to_download.items():
        filename = url.split("/")[-1]
        local_path = output_dir / filename

        logger.info(f"Downloading {file_type}: {filename}")
        logger.debug(f"URL: {url}")
        logger.debug(f"Destination: {local_path}")

        success = download_file(url, str(local_path))
        results[file_type] = success

    # Summary
    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful

    logger.info("=" * 60)
    logger.info(f"Download Summary: {successful}/{len(results)} successful")
    if failed > 0:
        logger.warning(f"Failed downloads: {failed}")
        failed_files = [k for k, v in results.items() if not v]
        logger.warning(f"Failed files: {failed_files}")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    # Example usage for testing
    from utils.config import load_config

    # Setup logger with DEBUG level for testing
    logger = setup_logger(__name__, level=logging.DEBUG)

    config = load_config()

    # Download only stock files for testing
    results = download_insee_files(
        config, files_to_download=["stock_etablissement", "stock_unitelegale"]
    )
