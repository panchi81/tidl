"""Tests for the TidlClient authentication and session management."""

from unittest.mock import MagicMock, patch

import pytest
from src.client import TidlClient
from src.exceptions import AuthenticationError


class TestTidlClient:
    """Test cases for TidlClient class."""

    def test_singleton_pattern(self) -> None:
        """Test that TidlClient follows singleton pattern."""
        client1 = TidlClient()
        client2 = TidlClient()
        assert client1 is client2, "TidlClient should be a singleton"

    def test_initial_state(self) -> None:
        """Test client initial state."""
        client = TidlClient()
        assert client.session is None, "Session should be None initially"
        assert not client.is_authenticated, "Should not be authenticated initially"

    @patch("src.client.Session")
    def test_authenticate_success(self, mock_session_class: MagicMock) -> None:
        """Test successful authentication."""
        # Setup mock
        mock_session = MagicMock()
        mock_session.login_oauth_simple.return_value = True
        mock_session.check_login.return_value = True
        mock_session_class.return_value = mock_session

        client = TidlClient()
        client.authenticate()

        assert client.session is mock_session
        assert client.is_authenticated
        mock_session.login_oauth_simple.assert_called_once()

    @patch("src.client.Session")
    def test_authenticate_failure(self, mock_session_class: MagicMock) -> None:
        """Test authentication failure."""
        # Setup mock
        mock_session = MagicMock()
        mock_session.login_oauth_simple.return_value = False
        mock_session_class.return_value = mock_session

        client = TidlClient()

        with pytest.raises(AuthenticationError, match="Failed to authenticate"):
            client.authenticate()

        assert client.session is None
        assert not client.is_authenticated

    @patch("src.client.Session")
    def test_get_session_without_auth(self, mock_session_class: MagicMock) -> None:
        """Test getting session without authentication."""
        client = TidlClient()

        with pytest.raises(AuthenticationError, match="Client not authenticated"):
            client.get_session()

    @patch("src.client.Session")
    def test_get_session_with_auth(self, mock_session_class: MagicMock) -> None:
        """Test getting session after authentication."""
        # Setup mock
        mock_session = MagicMock()
        mock_session.login_oauth_simple.return_value = True
        mock_session.check_login.return_value = True
        mock_session_class.return_value = mock_session

        client = TidlClient()
        client.authenticate()

        session = client.get_session()
        assert session is mock_session
