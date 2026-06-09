"""
test_download.py
──────────────────────────────────────────────────────────────────
Unit tests for src/ingestion/download_data.py.

All HTTP calls are mocked — no network required.
──────────────────────────────────────────────────────────────────
"""

import pytest
import requests as req
from pathlib import Path
from unittest.mock import MagicMock, patch

from ingestion.download_data import download_file, _progress_bar


# ── _progress_bar ─────────────────────────────────────────────────────────────

def test_progress_bar_silent_when_total_is_zero(capsys):
    """Should print nothing if total bytes is 0 (unknown Content-Length)."""
    _progress_bar(downloaded=0, total=0)
    captured = capsys.readouterr()
    assert captured.out == ""


def test_progress_bar_renders_brackets_and_numbers(capsys):
    """Should render a bar with [ ], MB numbers when total > 0."""
    _progress_bar(downloaded=500_000, total=1_000_000)
    captured = capsys.readouterr()
    assert "[" in captured.out
    assert "]" in captured.out
    assert "MB" in captured.out


def test_progress_bar_full_at_100_percent(capsys):
    """At 100 % the bar should contain no dashes."""
    _progress_bar(downloaded=1_000_000, total=1_000_000)
    captured = capsys.readouterr()
    assert "-" not in captured.out


# ── download_file: skip logic ─────────────────────────────────────────────────

def test_download_skips_if_file_already_exists(tmp_path):
    """If the destination already exists, return True immediately."""
    dest = tmp_path / "already_there.parquet"
    dest.write_bytes(b"existing data")

    result = download_file(
        url="http://example.com/irrelevant.parquet",
        destination=dest,
        chunk_size=8192,
        timeout=30,
    )

    assert result is True
    # File should not be modified
    assert dest.read_bytes() == b"existing data"


# ── download_file: success path ───────────────────────────────────────────────

def _make_mock_response(content: bytes):
    """Returns a context-manager-compatible mock HTTP response."""
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    mock.raise_for_status = MagicMock()
    mock.headers = {"Content-Length": str(len(content))}
    mock.iter_content = MagicMock(return_value=[content])
    return mock


def test_download_creates_file_on_success(tmp_path):
    """A successful response should write the file to disk."""
    dest = tmp_path / "file.parquet"
    payload = b"parquet data bytes"

    with patch("ingestion.download_data.requests.get", return_value=_make_mock_response(payload)):
        result = download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert result is True
    assert dest.exists()
    assert dest.read_bytes() == payload


def test_download_no_temp_file_left_on_success(tmp_path):
    """After a successful download the .tmp file should have been renamed away."""
    dest = tmp_path / "file.parquet"
    tmp = dest.with_suffix(".tmp")
    payload = b"data"

    with patch("ingestion.download_data.requests.get", return_value=_make_mock_response(payload)):
        download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert not tmp.exists()


# ── download_file: error paths ────────────────────────────────────────────────

def test_download_returns_false_on_http_error(tmp_path):
    """HTTP 4xx/5xx should log and return False without leaving any file."""
    dest = tmp_path / "file.parquet"

    mock = _make_mock_response(b"")
    mock.raise_for_status.side_effect = req.exceptions.HTTPError("404 Not Found")

    with patch("ingestion.download_data.requests.get", return_value=mock):
        result = download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert result is False
    assert not dest.exists()
    assert not dest.with_suffix(".tmp").exists()


def test_download_returns_false_on_timeout(tmp_path):
    """A Timeout should return False without leaving any file."""
    dest = tmp_path / "file.parquet"

    with patch(
        "ingestion.download_data.requests.get",
        side_effect=req.exceptions.Timeout(),
    ):
        result = download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert result is False
    assert not dest.exists()


def test_download_returns_false_on_connection_error(tmp_path):
    """A ConnectionError should return False without leaving any file."""
    dest = tmp_path / "file.parquet"

    with patch(
        "ingestion.download_data.requests.get",
        side_effect=req.exceptions.ConnectionError(),
    ):
        result = download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert result is False
    assert not dest.exists()


def test_download_returns_false_on_unexpected_error(tmp_path):
    """Any unexpected exception should return False cleanly."""
    dest = tmp_path / "file.parquet"

    with patch(
        "ingestion.download_data.requests.get",
        side_effect=RuntimeError("unexpected"),
    ):
        result = download_file("http://example.com/file.parquet", dest, 8192, 30)

    assert result is False
    assert not dest.exists()
