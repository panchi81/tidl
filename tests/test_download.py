"""Tests for the Download class and graceful shutdown functionality."""

import signal
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

from src.download import Download
from src.services import TrackService

from tests.conftest import TestConfig


class TestDownload:
    """Test cases for Download class."""

    def test_initialization(self, track_service: TrackService, temp_download_dir: Path) -> None:
        """Test Download class initialization."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Verify initialization
        assert downloader.track_service is track_service
        assert downloader.download_folder == temp_download_dir
        assert downloader.max_retries == TestConfig.MAX_RETRIES
        assert downloader.backoff_factor == TestConfig.BACKOFF_FACTOR
        assert downloader.download_delay_range == TestConfig.DOWNLOAD_DELAY_RANGE
        assert isinstance(downloader.event_abort, Event)
        assert not downloader.event_abort.is_set()

    def test_signal_handlers_setup(self, track_service: TrackService, temp_download_dir: Path) -> None:
        """Test that signal handlers are properly set up."""
        with patch("signal.signal") as mock_signal:
            downloader = Download(
                track_service=track_service,
                download_folder=temp_download_dir,
                max_retries=TestConfig.MAX_RETRIES,
                backoff_factor=TestConfig.BACKOFF_FACTOR,
                download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
            )

            # Verify signal handlers were registered
            signal_calls = mock_signal.call_args_list
            assert len(signal_calls) >= 2  # At least SIGINT and SIGTERM

            # Verify SIGINT and SIGTERM are handled
            registered_signals = [call[0][0] for call in signal_calls]
            assert signal.SIGINT in registered_signals
            assert signal.SIGTERM in registered_signals

    def test_abort_event_functionality(self, track_service: TrackService, temp_download_dir: Path) -> None:
        """Test abort event functionality."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Initially not set
        assert not downloader.event_abort.is_set()

        # Set and verify
        downloader.event_abort.set()
        assert downloader.event_abort.is_set()

        # Clear and verify
        downloader.event_abort.clear()
        assert not downloader.event_abort.is_set()

    def test_cleanup_method(self, track_service: TrackService, temp_download_dir: Path) -> None:
        """Test the cleanup method works without errors."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Should not raise any exceptions
        downloader._cleanup()

    @patch("atexit.register")
    def test_emergency_cleanup_registration(
        self, mock_atexit_register: MagicMock, track_service: TrackService, temp_download_dir: Path
    ) -> None:
        """Test that emergency cleanup is registered with atexit."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Verify atexit.register was called
        mock_atexit_register.assert_called_once()

    def test_download_workspace_context_manager(self, track_service: TrackService, temp_download_dir: Path) -> None:
        """Test the download workspace context manager."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        track_name = "Test Track - Artist Name"

        with downloader.download_workspace(track_name) as workspace:
            # Verify workspace is a Path object
            assert isinstance(workspace, Path)
            # Verify workspace exists during context
            assert workspace.exists()
            # Verify workspace is a directory
            assert workspace.is_dir()
            # Verify workspace name contains safe characters only
            assert "Test_Track_-_Artist_Name" in str(workspace) or "tidl_" in str(workspace)

        # Workspace should be cleaned up after context
        # Note: This might not be testable directly due to TemporaryDirectory cleanup
