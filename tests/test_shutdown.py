"""Tests for graceful shutdown functionality integration."""

import signal
from pathlib import Path
from unittest.mock import patch

from src.download import Download

from tests.conftest import TestConfig


class TestGracefulShutdown:
    """Integration tests for graceful shutdown functionality."""

    def test_graceful_shutdown_integration(self, track_service, temp_download_dir: Path) -> None:
        """Test complete graceful shutdown integration."""
        # Initialize downloader
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Verify initial state
        assert not downloader.event_abort.is_set()

        # Test abort event functionality
        downloader.event_abort.set()
        assert downloader.event_abort.is_set()

        # Test cleanup method doesn't raise exceptions
        downloader._cleanup()

    @patch("signal.signal")
    def test_signal_handlers_registration(self, mock_signal, track_service, temp_download_dir: Path) -> None:
        """Test that all expected signal handlers are registered."""
        # Initialize downloader (triggers signal handler setup)
        Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Get all signal registration calls
        signal_calls = mock_signal.call_args_list
        registered_signals = [call[0][0] for call in signal_calls]

        # Verify essential signals are registered
        assert signal.SIGINT in registered_signals
        assert signal.SIGTERM in registered_signals

        # Verify minimum number of signals
        assert len(signal_calls) >= 2

    def test_shutdown_workflow(self, track_service, temp_download_dir: Path) -> None:
        """Test the complete shutdown workflow."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Simulate shutdown sequence
        # 1. Signal received (simulated by setting abort event)
        downloader.event_abort.set()

        # 2. Abort event should be set
        assert downloader.event_abort.is_set()

        # 3. Cleanup should execute without errors
        downloader._cleanup()

        # 4. System should be in shutdown state
        assert downloader.event_abort.is_set()

    @patch("atexit.register")
    def test_emergency_cleanup_workflow(self, mock_atexit_register, track_service, temp_download_dir: Path) -> None:
        """Test emergency cleanup registration and workflow."""
        downloader = Download(
            track_service=track_service,
            download_folder=temp_download_dir,
            max_retries=TestConfig.MAX_RETRIES,
            backoff_factor=TestConfig.BACKOFF_FACTOR,
            download_delay_range=TestConfig.DOWNLOAD_DELAY_RANGE,
        )

        # Verify emergency cleanup was registered
        mock_atexit_register.assert_called_once()

        # Get the registered emergency cleanup function
        emergency_cleanup = mock_atexit_register.call_args[0][0]

        # Test emergency cleanup when abort event is not set
        downloader.event_abort.clear()
        emergency_cleanup()  # Should set abort event and call cleanup

        assert downloader.event_abort.is_set()

        # Test emergency cleanup when abort event is already set
        downloader.event_abort.set()
        # Should not raise any exceptions
        emergency_cleanup()
