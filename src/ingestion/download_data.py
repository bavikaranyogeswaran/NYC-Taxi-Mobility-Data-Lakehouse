"""
download_data.py
─────────────────────────────────────────────────────────────────
Ingestion script — Phase 1, Task 1.5

Downloads the NYC TLC Yellow Taxi Trip Records (Jan–Mar 2024)
and the Taxi Zone Lookup CSV from the official TLC CDN into the
local data/raw/ directory.

Design choices:
  - Stream downloads in small chunks (8 KB) so large Parquet files
    (~50-60 MB each) never fully load into RAM.
  - Skip files that already exist on disk — safe to re-run without
    re-downloading.
  - Print a live progress bar using only the standard library.
  - Log every download attempt to logs/ingestion.log for traceability.

Usage:
    python src/ingestion/download_data.py
─────────────────────────────────────────────────────────────────
"""

import sys
import logging
import requests
from pathlib import Path
from datetime import datetime

# ── Resolve project root so this script can be run from any working directory ─
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config import (
    RAW_YELLOW_TAXI_DIR,
    RAW_TAXI_ZONES_DIR,
    LOGS_DIR,
    YELLOW_TAXI_URLS,
    TAXI_ZONE_URLS,
    PIPELINE_SETTINGS,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOGS_DIR / "ingestion.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: print a terminal progress bar (no third-party lib required)
# ─────────────────────────────────────────────────────────────────────────────
def _progress_bar(downloaded: int, total: int, bar_width: int = 40) -> None:
    """Prints an in-place progress bar to stdout."""
    if total <= 0:
        return
    pct = downloaded / total
    filled = int(bar_width * pct)
    bar = "#" * filled + "-" * (bar_width - filled)
    mb_done = downloaded / 1_000_000
    mb_total = total / 1_000_000
    print(f"\r    [{bar}] {mb_done:.1f}/{mb_total:.1f} MB", end="", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Core: stream download a single file
# ─────────────────────────────────────────────────────────────────────────────
def download_file(url: str, destination: Path, chunk_size: int, timeout: int) -> bool:
    """
    Downloads a file from `url` and saves it to `destination`.

    Args:
        url         : Full HTTPS URL of the file to download.
        destination : Local Path where the file will be saved.
        chunk_size  : Number of bytes per chunk (keeps RAM usage low).
        timeout     : Max seconds to wait for the server to respond.

    Returns:
        True if the download succeeded, False if it failed.
    """
    # Skip if already downloaded — safe to re-run
    if destination.exists():
        size_mb = destination.stat().st_size / 1_000_000
        logger.info(f"SKIP  {destination.name} already exists ({size_mb:.1f} MB)")
        return True

    logger.info(f"START downloading {destination.name} from {url}")
    start_time = datetime.now()

    try:
        # stream=True means the response body is not immediately downloaded
        # — we pull it in chunks below, keeping memory low
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()  # raise HTTPError for 4xx / 5xx codes

            total_bytes = int(response.headers.get("Content-Length", 0))

            # Write to a temp file first; rename only on success
            # This prevents partial/corrupt files from masquerading as complete ones
            tmp_path = destination.with_suffix(".tmp")
            downloaded_bytes = 0

            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive empty chunks
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        _progress_bar(downloaded_bytes, total_bytes)

            print()  # newline after progress bar

            # Atomic rename: only appears at final destination if fully written
            tmp_path.rename(destination)

            elapsed = (datetime.now() - start_time).total_seconds()
            size_mb = destination.stat().st_size / 1_000_000
            logger.info(
                f"DONE  {destination.name} — {size_mb:.1f} MB in {elapsed:.1f}s"
            )
            return True

    except requests.exceptions.Timeout:
        logger.error(f"FAIL  {destination.name} — request timed out after {timeout}s")
    except requests.exceptions.HTTPError as e:
        logger.error(f"FAIL  {destination.name} — HTTP error: {e}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"FAIL  {destination.name} — connection error: {e}")
    except Exception as e:
        logger.error(f"FAIL  {destination.name} — unexpected error: {e}")

    # Clean up temp file if something went wrong
    tmp_path = destination.with_suffix(".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator: download all configured files
# ─────────────────────────────────────────────────────────────────────────────
def run_downloads() -> None:
    """
    Downloads all NYC TLC dataset files defined in config.py.

    File groups:
      1. Yellow Taxi Parquet files (Jan-Mar 2024) -> data/raw/yellow_taxi/
      2. Taxi Zone Lookup CSV                    -> data/raw/taxi_zones/
    """
    chunk_size = PIPELINE_SETTINGS["download_chunk_size_bytes"]
    timeout    = PIPELINE_SETTINGS["download_timeout_seconds"]

    # Ensure raw subdirectories exist
    RAW_YELLOW_TAXI_DIR.mkdir(parents=True, exist_ok=True)
    RAW_TAXI_ZONES_DIR.mkdir(parents=True, exist_ok=True)

    # Build a combined download manifest: {destination_path: url}
    download_manifest = {
        **{RAW_YELLOW_TAXI_DIR / filename: url for filename, url in YELLOW_TAXI_URLS.items()},
        **{RAW_TAXI_ZONES_DIR  / filename: url for filename, url in TAXI_ZONE_URLS.items()},
    }

    logger.info("=" * 60)
    logger.info("NYC Taxi Lakehouse — Data Ingestion")
    logger.info(f"Files to download : {len(download_manifest)}")
    logger.info(f"Log file          : {log_file}")
    logger.info("=" * 60)

    results = {}
    for destination, url in download_manifest.items():
        print(f"\n  Downloading: {destination.name}")
        success = download_file(url, destination, chunk_size, timeout)
        results[destination.name] = success

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Download Summary")
    print("=" * 60)
    succeeded = [name for name, ok in results.items() if ok]
    failed    = [name for name, ok in results.items() if not ok]

    for name in succeeded:
        print(f"  [OK]   {name}")
    for name in failed:
        print(f"  [FAIL] {name}")

    print(f"\n  Total : {len(results)}  |  OK: {len(succeeded)}  |  Failed: {len(failed)}")

    if failed:
        logger.error(f"Ingestion completed with {len(failed)} failure(s).")
        sys.exit(1)
    else:
        logger.info("Ingestion completed successfully. All files downloaded.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_downloads()
