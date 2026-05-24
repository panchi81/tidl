"""Protocol definitions for dependency inversion.

These protocols define the interfaces that concrete implementations must satisfy,
enabling testability and loose coupling between components.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from tidalapi.media import Track

from src.stream_info import StreamInfo
from src.track_metadata import TrackMetaData


class DownloadTracker(Protocol):
    """Interface for tracking download state (abstracts database operations)."""

    def is_track_downloaded(self, track: Track) -> bool:
        """Check if a track has been downloaded."""
        ...

    def get_best_quality_downloaded(self, track: Track) -> str | None:
        """Get the best quality string for a previously downloaded track."""
        ...

    def should_upgrade_quality(self, track: Track, new_quality: str) -> bool:
        """Check if the new quality warrants a re-download."""
        ...

    def mark_track_downloaded(
        self,
        track: Track,
        file_path: str,
        file_size: int | None = None,
        quality: str = "",
        has_metadata: bool = False,
    ) -> None:
        """Record that a track has been downloaded."""
        ...


class Decryptor(Protocol):
    """Interface for file decryption operations."""

    def decrypt_security_token(self, security_token: str) -> tuple[bytes, bytes]:
        """Decrypt a security token into a (key, nonce) pair."""
        ...

    def decrypt_file(self, encrypted_path: Path, decrypted_path: Path, key: bytes, nonce: bytes) -> None:
        """Decrypt an encrypted audio file."""
        ...


class MetadataService(Protocol):
    """Interface for writing metadata to audio files."""

    def write(self, file_path: Path, metadata: TrackMetaData) -> bool:
        """Write metadata (tags + cover) to an audio file. Returns success."""
        ...


class StreamStrategy(Protocol):
    """Interface for downloading a stream (standard or DASH)."""

    def download(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download stream content to workspace. Returns path to downloaded file, or None on failure."""
        ...
