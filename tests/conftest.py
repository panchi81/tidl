"""Test configuration and fixtures for tidl tests."""

import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from src.client import TidlClient
from src.services import TrackService


@pytest.fixture
def temp_download_dir() -> Iterator[Path]:
    """Provide a temporary download directory for tests."""
    with tempfile.TemporaryDirectory(prefix="tidl_test_") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_client() -> TidlClient:
    """Provide a mock TidlClient for testing."""
    return TidlClient()


@pytest.fixture
def track_service(mock_client: TidlClient) -> TrackService:
    """Provide a TrackService instance for testing."""
    return TrackService(mock_client)


class TestConfig:
    """Test configuration constants."""

    MAX_RETRIES = 1
    BACKOFF_FACTOR = 1.0
    DOWNLOAD_DELAY_RANGE = (0.1, 0.2)
    TIMEOUT_SECONDS = 5
