"""Authentication handling for TIDAL sessions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import requests
from loguru import logger
from tidalapi import Quality

if TYPE_CHECKING:
    from src.client import TidlSession


class Authenticator:
    """Handle TIDAL authentication flows (OAuth, PKCE)."""

    def __init__(self, session: TidlSession) -> None:
        self.session = session
        self._authenticated = False

    @property
    def is_authenticated(self) -> bool:
        """Check if the session is authenticated."""
        if not self._authenticated:
            return False
        try:
            return self.session.check_login()
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("Session check failed: {}", e)
            return False

    def authenticate_oauth(self) -> bool:
        """Authenticate using OAuth."""
        try:
            self.session.login_oauth_simple()
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("OAuth authentication error: {}", e)
            return False

        return self._verify_and_configure()

    def authenticate_pkce(self) -> bool:
        """Authenticate using PKCE method for HiRes/lossless access."""
        try:
            self.session.login_pkce()
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("PKCE authentication error: {}", e)
            return False

        return self._verify_and_configure()

    def _verify_and_configure(self) -> bool:
        """Verify login and set highest available quality."""
        try:
            if not self.session.check_login():
                logger.error("Login verification failed")
                return False
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("Session verification failed: {}", e)
            return False

        self._authenticated = True
        self._set_highest_available_quality()
        logger.info("Authentication successful")
        return True

    def _set_highest_available_quality(self) -> None:
        """Set the highest available audio quality."""
        quality_preferences = [Quality.hi_res_lossless, Quality.high_lossless, Quality.low_320k, Quality.low_96k]

        for quality in quality_preferences:
            try:
                self.session.audio_quality = quality
            except (AttributeError, ValueError):
                logger.debug("Quality {} not available", quality.name)
                continue
            else:
                logger.debug("Session quality set to: {}", quality.name)
                return

        self.session.audio_quality = Quality.low_320k
        logger.warning("Using fallback quality: {}", Quality.low_320k.name)
