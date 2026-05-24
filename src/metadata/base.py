"""Base metadata writer — dispatcher and format detection."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import mutagen
import mutagen.flac
import mutagen.mp3
import mutagen.mp4
from loguru import logger

from src.track_metadata import TrackMetaData


class FormatWriter(Protocol):
    """Protocol for format-specific tag writers."""

    def write_tags(self, m: mutagen.FileType, metadata: TrackMetaData) -> None:
        """Write format-specific tags."""
        ...

    def add_cover(self, m: mutagen.FileType, image_data: bytes) -> None:
        """Add cover image in format-specific way."""
        ...


class MetadataWriter:
    """Write metadata to audio files, dispatching to format-specific writers."""

    def __init__(self, path_file: Path) -> None:
        self.path_file = path_file
        self.m = self._load_file()

    def write(self, metadata: TrackMetaData) -> bool:
        """Write metadata (tags + cover) to the audio file.

        Returns:
            True on success, False on failure.

        """
        if self.m is None:
            logger.error("Could not load file: {}", self.path_file)
            return False

        try:
            if not self.m.tags:
                self.m.add_tags()

            writer = self._get_format_writer()
            if writer is None:
                logger.warning("Unsupported file format: {}", type(self.m))
                return False

            writer.write_tags(self.m, metadata)

            if metadata.cover:
                writer.add_cover(self.m, metadata.cover)

            self.m.save()
            logger.debug("Successfully wrote metadata to: {}", self.path_file.name)
            return True

        except Exception as e:
            logger.error("Failed to write metadata to {}: {}", self.path_file.name, e)
            return False

    def _get_format_writer(self) -> FormatWriter | None:
        """Select the appropriate format writer based on detected file type."""
        from src.metadata.flac import FlacWriter
        from src.metadata.mp3 import Mp3Writer
        from src.metadata.mp4 import Mp4Writer

        if isinstance(self.m, mutagen.flac.FLAC):
            return FlacWriter()
        if isinstance(self.m, mutagen.mp4.MP4):
            return Mp4Writer()
        if isinstance(self.m, (mutagen.mp3.MP3, mutagen.id3.ID3)):
            return Mp3Writer()
        return None

    def _load_file(self) -> mutagen.FileType | None:
        """Load file with format detection based on file header."""
        try:
            with self.path_file.open("rb") as f:
                header = f.read(16)

            file_ext = self.path_file.suffix.lower()

            if b"ftyp" in header:
                logger.debug("Detected MP4 container (file ext: {})", file_ext)
                return mutagen.mp4.MP4(str(self.path_file))

            if header.startswith(b"fLaC"):
                logger.debug("Detected FLAC file (file ext: {})", file_ext)
                return mutagen.flac.FLAC(str(self.path_file))

            if header.startswith((b"ID3", b"\xff\xfb", b"\xff\xf3")):
                logger.debug("Detected MP3 file (file ext: {})", file_ext)
                return mutagen.id3.ID3(str(self.path_file))

            logger.debug("Using standard mutagen detection for {} (header: {})", file_ext, header[:8].hex())
            return mutagen.File(str(self.path_file))

        except Exception as e:
            logger.error("Failed to load file {}: {}", self.path_file, e)
            return None
