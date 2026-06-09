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
from utils.logging_utils import get_logger

log = get_logger(__name__, LOGS_DIR / "ingestion.log")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: print a terminal progress bar (no third-party lib required)
# ─────────────────────────────────────────────────────────────────────────────
def _progress_bar(downloaded: int, total: int, bar_width: int = 40) -> None:
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
    if destination.exists():
        size_mb = destination.stat().st_size / 1_000_000
        log.info("download_skipped", file=destination.name, size_mb=round(size_mb, 1))
        return True

    log.info("download_start", file=destination.name, url=url)
    start_time = datetime.now()

    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()

            total_bytes = int(response.headers.get("Content-Length", 0))
            tmp_path = destination.with_suffix(".tmp")
            downloaded_bytes = 0

            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        _progress_bar(downloaded_bytes, total_bytes)

            print()  # newline after progress bar
            tmp_path.rename(destination)

            elapsed = (datetime.now() - start_time).total_seconds()
            size_mb = destination.stat().st_size / 1_000_000
            log.info("download_done", file=destination.name, size_mb=round(size_mb, 1), duration_seconds=round(elapsed, 1))
            return True

    except requests.exceptions.Timeout:
        log.error("download_failed", file=destination.name, reason="timeout", timeout_seconds=timeout)
    except requests.exceptions.HTTPError as e:
        log.error("download_failed", file=destination.name, reason="http_error", error=str(e))
    except requests.exceptions.ConnectionError as e:
        log.error("download_failed", file=destination.name, reason="connection_error", error=str(e))
    except Exception as e:
        log.error("download_failed", file=destination.name, reason="unexpected_error", error=str(e))

    tmp_path = destination.with_suffix(".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator: download all configured files
# ─────────────────────────────────────────────────────────────────────────────
def run_downloads() -> None:
    chunk_size = PIPELINE_SETTINGS["download_chunk_size_bytes"]
    timeout    = PIPELINE_SETTINGS["download_timeout_seconds"]

    RAW_YELLOW_TAXI_DIR.mkdir(parents=True, exist_ok=True)
    RAW_TAXI_ZONES_DIR.mkdir(parents=True, exist_ok=True)

    download_manifest = {
        **{RAW_YELLOW_TAXI_DIR / filename: url for filename, url in YELLOW_TAXI_URLS.items()},
        **{RAW_TAXI_ZONES_DIR  / filename: url for filename, url in TAXI_ZONE_URLS.items()},
    }

    log.info("ingestion_start", file_count=len(download_manifest))

    results = {}
    for destination, url in download_manifest.items():
        print(f"\n  Downloading: {destination.name}")
        success = download_file(url, destination, chunk_size, timeout)
        results[destination.name] = success

    succeeded = [name for name, ok in results.items() if ok]
    failed    = [name for name, ok in results.items() if not ok]

    print("\n" + "=" * 60)
    print("Download Summary")
    print("=" * 60)
    for name in succeeded:
        print(f"  [OK]   {name}")
    for name in failed:
        print(f"  [FAIL] {name}")
    print(f"\n  Total : {len(results)}  |  OK: {len(succeeded)}  |  Failed: {len(failed)}")

    if failed:
        log.error("ingestion_failed", failed_count=len(failed), failed_files=failed)
        sys.exit(1)
    else:
        log.info("ingestion_complete", file_count=len(succeeded))


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_downloads()
