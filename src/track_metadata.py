"""Track metadata dataclass — wraps tidalapi Track into tagging-ready data."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from loguru import logger
from tidalapi import Track
from tidalapi.artist import Artist, Role

MAX_COVER_IMAGE_SIZE = 3000


@dataclass
class TrackMetaData:
    """Enhanced metadata wrapper for tidalapi Track."""

    # Core metadata
    title: str = ""
    artists: str = ""
    album: str = ""
    isrc: str = ""

    # Basic track info
    date: str = ""
    year: str = ""
    length: int = 0

    # Musical data
    bpm: int = 0
    key: str = ""
    key_scale: str = ""

    # File related data
    cover: bytes = b""

    @property
    def full_title(self) -> str:
        """Return artist - title format."""
        return f"{self.artists} - {self.title}"

    @classmethod
    def from_track(cls, track: Track) -> TrackMetaData:
        """Create TrackMetaData from a tidalapi Track."""
        cover_data = cls._download_cover_image(track)

        return cls(
            title=track.name or "",
            artists=cls._name_builder_artists(track),
            album=track.album.name if track.album and track.album.name else "",
            isrc=track.isrc or "",
            length=track.duration or 0,
            date=track.album.available_release_date.strftime("%Y-%m-%d")
            if track.album and track.album.available_release_date
            else "",
            year=track.album.year if track.album and track.album.year else "",
            cover=cover_data,
            bpm=track.bpm or 0,
            key=track.key or "",
            key_scale=track.key_scale or "",
        )

    @classmethod
    def _download_cover_image(cls, track: Track) -> bytes:
        """Download cover image, trying sizes 1280 -> 640 -> 320."""
        if not track.album:
            logger.debug("No album available for cover image")
            return b""

        quality_preferences = [1280, 640, 320]
        for quality in quality_preferences:
            try:
                cover_url = track.album.image(quality)
                logger.debug("Attempting to download cover: {} ({}px)", cover_url, quality)
                with httpx.Client(timeout=30.0) as client:
                    response = client.get(cover_url)
                    response.raise_for_status()
                    if response.headers.get("content-type", "").startswith("image/"):
                        image_data = response.content
                        logger.debug("Downloaded cover image: {} bytes ({}px)", len(image_data), quality)
                        return image_data
                    logger.warning("Invalid image content type: {}", response.headers.get("content-type"))
            except httpx.HTTPError:
                logger.warning("HTTP error downloading cover at {} quality", quality)
                continue
            except OSError:
                logger.warning("OS error downloading cover at {} quality", quality)
                continue

        logger.warning("Failed to download cover image for track: {}", track.name)
        return b""

    @classmethod
    def _name_builder_artists(cls, track: Track) -> str:
        """Format all artists for tags."""
        return "; ".join(artist.name for artist in track.artists)

    @classmethod
    def _name_builder_album_artist(cls, track: Track) -> str:
        """Format main album artists."""
        artists: list[Artist] = track.album.artists if isinstance(track, Track) else track.artists
        artists_tmp = [artist.name for artist in artists if Role.main in artist.roles]
        return "; ".join(artists_tmp)
