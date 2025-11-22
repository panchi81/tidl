"""Database module for tracking downloads, metadata, and library state.

Uses SQLite via Python's built-in sqlite3 module.
"""

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, Unpack

from loguru import logger
from tidalapi.media import Track

from src.exceptions import DBError
from src.setup_logging import setup_logging

DB_PATH = Path(__file__).parent.parent / "downloads.db"

setup_logging()


class DownloadMetadata(TypedDict, total=False):
    """Additional metadata for download records."""

    file_size: int
    codec: str
    bit_depth: int
    sample_rate: int
    quality: str
    has_metadata: bool
    has_cover: bool
    checksum: str


SCHEMA = """
CREATE TABLE IF NOT EXISTS albums (
    id TEXT PRIMARY KEY,
    title TEXT,
    artist_name TEXT,
    release_date TEXT,
    number_of_tracks INTEGER,
    number_of_volumes INTEGER,
    cover_url TEXT
);

CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    title TEXT,
    artist_name TEXT,
    album_id TEXT,
    album_name TEXT,
    track_number INTEGER,
    volume_number INTEGER,
    duration INTEGER,
    isrc TEXT,
    explicit BOOLEAN,
    audio_quality TEXT,
    audio_mode TEXT,
    media_metadata_tags TEXT,
    FOREIGN KEY(album_id) REFERENCES albums(id)
);

CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id TEXT,
    file_path TEXT,
    file_size INTEGER,
    file_extension TEXT,
    codec TEXT,
    bit_depth INTEGER,
    sample_rate INTEGER,
    downloaded_at TEXT,
    quality TEXT,
    has_metadata BOOLEAN,
    has_cover BOOLEAN,
    checksum TEXT,
    FOREIGN KEY(track_id) REFERENCES tracks(id)
);

CREATE TABLE IF NOT EXISTS playlists (
    id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    owner TEXT,
    last_synced TEXT,
    track_count INTEGER
);

CREATE TABLE IF NOT EXISTS playlist_tracks (
    playlist_id TEXT,
    track_id TEXT,
    position INTEGER,
    date_added TEXT,
    PRIMARY KEY (playlist_id, track_id),
    FOREIGN KEY(playlist_id) REFERENCES playlists(id),
    FOREIGN KEY(track_id) REFERENCES tracks(id)
);
"""


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a connection to the SQLite database.

    Args:
        db_path: Optional custom path to the database file.

    Returns:
        A SQLite connection with row factory configured.

    """
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database(db_path: Path | None = None) -> None:
    """Initialize the database with the required schema.

    Args:
        db_path: Optional custom path to the database file.

    """
    try:
        with get_connection(db_path) as conn:
            conn.executescript(SCHEMA)
        logger.debug("Database schema initialized successfully")
    except sqlite3.Error as e:
        logger.exception("Failed to initialize database schema: {}", e)
        raise


# --- Helper functions ---


def insert_track(track: dict[str, Any]) -> None:
    """Insert or update a track in the database.

    Args:
        track: Dictionary containing track metadata matching the schema.

    Raises:
        sqlite3.Error: If the database operation fails.

    """
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tracks (
                    id, title, artist_name, album_id, album_name, track_number,
                    volume_number, duration, isrc, explicit, audio_quality, audio_mode, media_metadata_tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    track["id"],
                    track["title"],
                    track["artist_name"],
                    track.get("album_id"),
                    track.get("album_name"),
                    track.get("track_number"),
                    track.get("volume_number"),
                    track.get("duration"),
                    track.get("isrc"),
                    track.get("explicit"),
                    track.get("audio_quality"),
                    track.get("audio_mode"),
                    json.dumps(track.get("media_metadata_tags", {})),
                ),
            )
            conn.commit()
    except sqlite3.Error as e:
        logger.exception("Failed to insert track {}: {}", track.get("id", "unknown"), e)
        raise


def insert_download(download: dict[str, Any]) -> None:
    """Insert a download record into the database.

    Args:
        download: Dictionary containing download metadata matching the schema.

    Raises:
        sqlite3.Error: If the database operation fails.

    """
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO downloads (
                    track_id, file_path, file_size, file_extension, codec, bit_depth, sample_rate, downloaded_at,
                    quality, has_metadata, has_cover, checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    download["track_id"],
                    download["file_path"],
                    download.get("file_size"),
                    download.get("file_extension"),
                    download.get("codec"),
                    download.get("bit_depth"),
                    download.get("sample_rate"),
                    download.get("downloaded_at"),
                    download.get("quality"),
                    download.get("has_metadata"),
                    download.get("has_cover"),
                    download.get("checksum"),
                ),
            )
            conn.commit()
    except sqlite3.Error as e:
        logger.exception("Failed to insert download for track {}: {}", download.get("track_id", "unknown"), e)
        raise


def track_exists(track_id: str) -> bool:
    """Check if a track exists in the database.

    Args:
        track_id: The TIDAL track ID.

    Returns:
        True if the track exists in the tracks table, False otherwise.

    """
    try:
        with get_connection() as conn:
            cur = conn.execute("SELECT 1 FROM tracks WHERE id = ?", (track_id,))
            return cur.fetchone() is not None
    except sqlite3.Error as e:
        logger.exception("Failed to check if track exists {}: {}", track_id, e)
        return False


def download_exists(track_id: str) -> bool:
    """Check if a download exists for a given track.

    Args:
        track_id: The TIDAL track ID.

    Returns:
        True if at least one download exists for this track, False otherwise.

    """
    try:
        with get_connection() as conn:
            cur = conn.execute("SELECT 1 FROM downloads WHERE track_id = ? LIMIT 1", (track_id,))
            return cur.fetchone() is not None
    except sqlite3.Error as e:
        logger.exception("Failed to check if download exists for track {}: {}", track_id, e)
        return False


def get_downloads_for_track(track_id: str) -> list[dict[str, Any]]:
    """Get all downloads for a given track ID.

    Args:
        track_id: The TIDAL track ID.

    Returns:
        List of download records as dictionaries.

    """
    try:
        with get_connection() as conn:
            cur = conn.execute("SELECT * FROM downloads WHERE track_id = ?", (track_id,))
            return [dict(row) for row in cur.fetchall()]
    except sqlite3.Error as e:
        logger.exception("Failed to get downloads for track {}: {}", track_id, e)
        return []


def track_to_dict(track: Track) -> dict[str, Any]:
    """Convert a TIDAL Track object to a dictionary matching the database schema.

    Args:
        track: A tidalapi Track object.

    Returns:
        Dictionary with track metadata ready for database insertion.

    """
    return {
        "id": str(track.id),
        "title": track.name,
        "artist_name": track.artist.name if track.artist else None,
        "album_id": str(track.album.id) if track.album else None,
        "album_name": track.album.name if track.album else None,
        "track_number": track.track_num,
        "volume_number": track.volume_num,
        "duration": track.duration,
        "isrc": track.isrc,
        "explicit": track.explicit,
        "audio_quality": track.audio_quality,
        "audio_mode": getattr(track, "audio_mode", None),
        "media_metadata_tags": getattr(track, "media_metadata_tags", []),
    }


# --- Class-based Interface ---


class DownloadDB:
    """Object-oriented interface for download tracking database operations."""

    def __init__(self, db_path: Path | None = None) -> None:
        """Initialize the DownloadDB instance.

        Args:
            db_path: Optional custom path to the database file.

        """
        self.db_path = db_path or DB_PATH
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        """Ensure the database schema is initialized."""
        try:
            initialize_database(self.db_path)
        except sqlite3.Error as e:
            logger.exception("Failed to ensure database initialization: {}", e)
            raise

    def is_track_downloaded(self, track: Track | str) -> bool:
        """Check if a track has been downloaded.

        Args:
            track: Either a Track object or a track ID string.

        Returns:
            True if the track has at least one download record, False otherwise.

        """
        track_id = str(track.id) if isinstance(track, Track) else track
        return download_exists(track_id)

    def mark_track_downloaded(  # noqa: PLR0913
        self,
        track: Track,
        file_path: str | Path,
        file_size: int | None = None,
        codec: str | None = None,
        bit_depth: int | None = None,
        sample_rate: int | None = None,
        quality: str | None = None,
        *,
        has_metadata: bool = False,
        has_cover: bool = False,
        checksum: str | None = None,
    ) -> None:
        """Mark a track as downloaded by inserting track and download records.

        Args:
            track: The TIDAL Track object.
            file_path: Path to the downloaded file.
            file_size: Size of the file in bytes.
            codec: Audio codec (e.g., 'flac', 'aac').
            bit_depth: Bit depth of the audio.
            sample_rate: Sample rate of the audio.
            quality: Quality tier (e.g., 'HiFi', 'Master').
            has_metadata: Whether metadata was added to the file.
            has_cover: Whether cover art was added to the file.
            checksum: Optional checksum for integrity verification.

        """
        try:
            # Insert or update track record
            self.insert_track_from_obj(track)

            # Insert download record
            file_path_obj = Path(file_path)
            download_dict = {
                "track_id": str(track.id),
                "file_path": str(file_path),
                "file_size": file_size,
                "file_extension": file_path_obj.suffix,
                "codec": codec,
                "bit_depth": bit_depth,
                "sample_rate": sample_rate,
                "downloaded_at": datetime.now(tz=UTC).isoformat(),
                "quality": quality,
                "has_metadata": has_metadata,
                "has_cover": has_cover,
                "checksum": checksum,
            }
            insert_download(download_dict)
            logger.debug("Marked track {} as downloaded", track.full_name)
        except Exception as e:
            logger.exception("Failed to mark track as downloaded {}: {}", track.full_name, e)
            raise

    def insert_track_from_obj(self, track: Track) -> None:
        """Insert or update a track record from a Track object.

        Args:
            track: The TIDAL Track object.

        """
        track_dict = track_to_dict(track)
        insert_track(track_dict)

    def insert_download_from_obj(self, track: Track, file_path: str | Path, **kwargs: Unpack[DownloadMetadata]) -> None:
        """Insert a download record from a Track object.

        Args:
            track: The TIDAL Track object.
            file_path: Path to the downloaded file.
            **kwargs: Additional download metadata (file_size, codec, bit_depth, sample_rate,
                quality, has_metadata, has_cover, checksum).

        """
        file_path_obj = Path(file_path)
        download_dict = {
            "track_id": str(track.id),
            "file_path": str(file_path),
            "file_extension": file_path_obj.suffix,
            "downloaded_at": datetime.now(tz=UTC).isoformat(),
            **kwargs,
        }
        insert_download(download_dict)

    def get_track_downloads(self, track: Track | str) -> list[dict[str, Any]]:
        """Get all download records for a track.

        Args:
            track: Either a Track object or a track ID string.

        Returns:
            List of download records as dictionaries.

        """
        track_id = str(track.id) if isinstance(track, Track) else track
        return get_downloads_for_track(track_id)

    def get_best_quality_downloaded(self, track: Track | str) -> str | None:
        """Get the best quality that has been downloaded for a track.

        Args:
            track: Either a Track object or a track ID string.

        Returns:
            The best quality string (e.g., 'hi_res_lossless', 'high_lossless'), or None if not downloaded.

        """
        downloads = self.get_track_downloads(track)
        if not downloads:
            return None

        # Quality ranking (higher is better) - supports both enum names and uppercase strings
        quality_rank = {
            "hi_res_lossless": 4,
            "hi_res": 4,
            "high_lossless": 3,
            "lossless": 3,
            "high": 2,
            "low_320k": 2,
            "low_96k": 1,
            "low": 1,
        }

        # Get the highest quality from all downloads
        best_quality = None
        best_rank = -1

        for download in downloads:
            if quality := download.get("quality"):
                # Normalize: convert to lowercase and replace spaces/underscores
                normalized = quality.lower().replace(" ", "_")
                rank = quality_rank.get(normalized, 0)
                if rank > best_rank:
                    best_rank = rank
                    best_quality = quality

        return best_quality

    def should_upgrade_quality(self, track: Track, new_quality: str) -> bool:
        """Check if a new quality is better than the existing downloaded quality.

        Args:
            track: The Track object to check.
            new_quality: The new quality string (from Quality enum name).

        Returns:
            True if the new quality is better, False otherwise.

        """
        existing_quality = self.get_best_quality_downloaded(track)
        if not existing_quality:
            return True  # No existing download, so yes, download it

        # Quality ranking (higher is better) - supports both enum names and uppercase strings
        quality_rank = {
            "hi_res_lossless": 4,
            "hi_res": 4,
            "high_lossless": 3,
            "lossless": 3,
            "high": 2,
            "low_320k": 2,
            "low_96k": 1,
            "low": 1,
        }

        # Normalize both qualities to lowercase with underscores
        existing_normalized = existing_quality.lower().replace(" ", "_")
        new_normalized = new_quality.lower().replace(" ", "_")

        existing_rank = quality_rank.get(existing_normalized, 0)
        new_rank = quality_rank.get(new_normalized, 0)

        return new_rank > existing_rank


if __name__ == "__main__":
    # Initialize database
    initialize_database()
    logger.info("Database initialized at {}", DB_PATH)

    # Basic validation
    try:
        db = DownloadDB()
        logger.info("DownloadDB instance created successfully")

        # Test table existence
        with get_connection() as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [t[0] for t in tables]
            expected_tables = {"albums", "tracks", "downloads", "playlists", "playlist_tracks"}

            if expected_tables.issubset(set(table_names)):
                logger.info("All expected tables exist: {}", table_names)
            else:
                missing = expected_tables - set(table_names)
                logger.error("Missing tables: {}", missing)

    except DBError:
        logger.exception("Validation failed: {}")
