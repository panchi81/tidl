from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

import requests
from loguru import logger
from tidalapi import Quality, Session, Track
from tidalapi.media import AudioMode, Codec

from src.exceptions import AuthError, PlaylistError
from src.setup_logging import setup_logging

setup_logging()

@dataclass
class TrackMetaData:
    """Metadata for a track."""

    # Required fields
    id: int
    name: str
    artist: str
    album: str
    duration: int  # seconds

    # Optional fields
    artist_id: int | None = None
    album_id: int | None = None
    track_number: int | None = None
    disc_number: int | None = None
    release_date: date | None = None
    release_year: int | None = None
    genre: str | None = None
    explicit: bool = False
    quality: Quality | None = None
    audio_mode: AudioMode | None = None
    codec: Codec | None = None
    media_metadata_tags: list[str] | None = None
    isrc: str | None = None  # International Standard Recording Code
    url: str | None = None  # TIDAL URL

    def __post_init__(self) -> None:
        """Extract year from release_date if available."""
        if self.release_date and not self.release_year:
            self.release_year = self.release_date.year

    @property
    def duration_formatted(self) -> str:
        """Return duration as MM:SS format."""
        minutes = self.duration // 60
        seconds = self.duration % 60
        return f"{minutes}:{seconds:02d}"

    @property
    def full_title(self) -> str:
        """Return artist - title format."""
        return f"{self.artist} - {self.name}"

    @property
    def is_hi_res(self) -> bool:
        """Check if the track is hi-res quality."""
        return self.quality in {Quality.high_lossless, Quality.hi_res_lossless}

    @classmethod
    def from_tidal_track(cls, track: Track) -> "TrackMetaData":
        """Create TrackMetaData from a track object."""
        # Map TIDAL'S quality string to tidalapi's Quality enum
        quality_map = {
            "LOW": Quality.low_96k,
            "HIGH": Quality.low_320k,
            "LOSSLESS": Quality.high_lossless,
            "HI_RES_LOSSLESS": Quality.hi_res_lossless,
        }

        return cls(
            id=track.id,
            name=track.name,
            artist=track.artist.name if track.artist else "Unknown Artist",
            artist_id=track.artist.id if track.artist else None,
            album=track.album.name if track.album else "Unknown Album",
            album_id=track.album.id if track.album else None,
            duration=track.duration or 0,
            release_date=track.album.release_date if track.album else None,
            release_year=getattr(track, "release_year", None),
            genre=getattr(track, "genre", None),
            explicit=getattr(track, "explicit", False),
            quality=quality_map.get(getattr(track, "audio_quality", None)),
            audio_mode=getattr(track, "audio_mode", None),
            codec=getattr(track, "codec", None),
            media_metadata_tags=getattr(track, "media_metadata_tags", None),
            isrc=getattr(track, "isrc", None),
            url=f"https://tidal.com/album/{track.album.id}/track/{track.id}" if track.album and track.id else None,
        )


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

    def authenticate(self) -> bool:
        """Authenticate the session."""
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

            return [TrackMetaData.from_tidal_track(track) for track in tracks]

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
