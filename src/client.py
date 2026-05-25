"""TIDAL client — session management and quality negotiation."""

from __future__ import annotations

from typing import ClassVar

import requests
from loguru import logger
from tidalapi.media import Quality, Track
from tidalapi.session import Session

from src.exceptions import StreamInfoError
from src.setup_logging import setup_logging

setup_logging()


class SingletonMeta(type):
    """Metaclass to ensure only one instance of a class is created."""

    _instances: ClassVar[dict[type, object]] = {}

    def __call__(cls, *args, **kwargs) -> object:
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class TidlSession(Session):
    """Extension of tidalapi.Session with utility methods."""

    def __init__(self) -> None:
        super().__init__()
        logger.debug("Session instance created.")

    def get_user_id(self) -> int:
        """Get the user ID of the logged-in user."""
        return self.user.id


class TidlClient(metaclass=SingletonMeta):
    """TIDAL API client — holds session and provides quality negotiation."""

    def __init__(self) -> None:
        self.session = TidlSession()
        logger.debug("Client instance created.")

    def get_track_with_quality(self, track: Track) -> tuple[Track, Quality]:
        """Get track stream at highest available quality.

        Tries qualities from highest to lowest, returning the track and actual quality obtained.
        """
        quality_preferences = [Quality.hi_res_lossless, Quality.high_lossless, Quality.low_320k, Quality.low_96k]
        original_quality = self.session.audio_quality

        for quality in quality_preferences:
            try:
                self.session.audio_quality = quality
                stream = track.get_stream()
                if stream:
                    logger.debug("Track {} available in quality: {}", track.name, quality)
                    return track, quality
            except (requests.RequestException, ValueError, OSError):
                logger.debug("Track {} not available in {}", track.name, quality)
                continue

        self.session.audio_quality = original_quality
        msg = f"Track {track.name} not available in any quality"
        raise StreamInfoError(msg)
