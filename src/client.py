from collections.abc import Iterator
from typing import ClassVar

import requests
from loguru import logger
from tidalapi import Session, Track

from src.exceptions import AuthError, PlaylistError
from src.setup_logging import setup_logging
from src.track_metadata import TrackMetaData

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
    """A simple extension of tidalapi.Session to add custom functionality."""

    def __init__(self) -> None:
        super().__init__()
        logger.debug("Session instance created.")

    def get_user_id(self) -> int:
        """Get the user ID of the logged-in user."""
        return self.user.id


class TidlClient(metaclass=SingletonMeta):
    """A simple TIDL API client."""

    def __init__(self) -> None:
        self.session = TidlSession()
        self._authenticated = False
        logger.debug("Client instance created.")

    def authenticate_oauth(self) -> bool:
        """Authenticate the session using OAuth."""
        try:
            logger.debug("Starting OAuth authentication...")
            login_result = self.session.login_oauth_simple()
            logger.debug("OAuth authentication result: {}", login_result)
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("Authentication error: {}", e)
            return False
        else:
            try:
                user_check = self.session.check_login()
            except (requests.RequestException, ValueError, OSError) as e:
                logger.error("Session verification failed: {}", e)
                return False
            else:
                if not user_check:
                    logger.error("User check failed: {}", user_check)
                    return False

                logger.debug("Session login check result: {}", user_check)
                self._authenticated = True
                logger.info("Authentication successful.")
                return True

    def authenticate_device(self) -> bool:
        """Authenticate the session using device authentication."""
        try:
            logger.debug("Starting device authentication...")
            login_result = self.session.login_device()
            logger.debug("Device authentication result: {}", login_result)
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("Authentication error: {}", e)
            return False
        else:
            try:
                user_check = self.session.check_login()
            except (requests.RequestException, ValueError, OSError) as e:
                logger.error("Session verification failed: {}", e)
                return False
            else:
                if not user_check:
                    logger.error("User check failed: {}", user_check)
                    return False

                logger.debug("Session login check result: {}", user_check)
                self._authenticated = True
                logger.info("Authentication successful.")
                return True

    def is_authenticated(self) -> bool:
        """Check if the session is authenticated."""
        auth_flag = getattr(self, "_authenticated", False)
        try:
            session_check = self.session.check_login()
        except (requests.RequestException, ValueError, OSError) as e:
            logger.error("Session check failed: {}", e)
            return False
        else:
            logger.debug("Auth flag: {}, Session check: {}", auth_flag, session_check)
            return auth_flag and session_check

    def get_playlist_tracks(self, playlist_id: str) -> list[Track]:
        """Get tracks from a playlist by its ID."""
        if not self.is_authenticated():
            logger.error("Cannot fetch playlist tracks: not authenticated.")
            msg = "Not authenticated"
            raise AuthError(msg)

        try:
            playlist = self.session.playlist(playlist_id)
        except (requests.RequestException, ValueError, KeyError, AttributeError) as e:
            logger.error("Error fetching playlist tracks: {}", e)
            msg = f"Failed to fetch playlist tracks: {e}"  # Fixed formatting
            raise PlaylistError(msg) from e
        else:
            tracks = playlist.tracks()
            playlist_name = playlist.name
            logger.info("Fetched {} tracks from playlist {}", len(tracks), playlist_name)
            return tracks

    def get_playlist_tracks_detailed(self, playlist_id: str) -> list[TrackMetaData]:
        """Fetch detailed track metadata for all tracks in a playlist."""
        if not self.is_authenticated():
            logger.error("Cannot fetch playlist tracks: not authenticated.")
            msg = "Not authenticated"
            raise AuthError(msg)

        try:
            playlist = self.session.playlist(playlist_id)
        except (requests.RequestException, ValueError, KeyError, OSError, AttributeError) as e:
            logger.error("Error fetching playlist {}: {}", playlist_id, e)
            msg = f"Failed to fetch playlist: {e}"
            raise PlaylistError(msg) from e
        else:
            tracks = playlist.tracks()
            playlist_name = playlist.name
            logger.info("Fetched playlist: {} with {} tracks", playlist_name, len(tracks))

            return [TrackMetaData.from_tidal_track(track, include_stream_url=True) for track in tracks]

    def get_track_info(self, tracks: list[str]) -> Iterator[str]:
        """Get track information for a list of track IDs."""
        if not self.is_authenticated():
            logger.error("Cannot fetch track info: not authenticated.")
            msg = "Not authenticated"
            raise AuthError(msg)

        for track_id in tracks:
            try:
                track = self.session.track(track_id)
            except (requests.RequestException, ValueError, KeyError, AttributeError) as e:
                logger.error("Error fetching track info for ID {}: {}", track_id, e)
                continue
            else:
                track_info = f"{track.artist.name} - {track.name}"
                logger.info("Fetched track info: {}", track_info)
                yield track_info
