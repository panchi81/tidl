"""FLAC metadata writer."""

from __future__ import annotations

import mutagen
import mutagen.flac
from loguru import logger
from mutagen.flac import Picture

from src.track_metadata import TrackMetaData


class FlacWriter:
    """Write metadata tags and cover image to FLAC files."""

    def write_tags(self, m: mutagen.flac.FLAC, metadata: TrackMetaData) -> None:
        """Write Vorbis comment tags to FLAC file."""
        m.tags["TITLE"] = metadata.title
        m.tags["ALBUM"] = metadata.album
        m.tags["ARTIST"] = metadata.artists
        m.tags["LENGTH"] = str(metadata.length)
        m.tags["DATE"] = metadata.date
        m.tags["YEAR"] = str(metadata.year)
        if metadata.isrc:
            m.tags["ISRC"] = metadata.isrc
        if metadata.bpm:
            m.tags["BPM"] = str(metadata.bpm)

        written_tags = "\n".join(f"{key}: {value}" for key, value in m.tags.items())
        logger.debug("FLAC tags written successfully: \n{}", written_tags)

    def add_cover(self, m: mutagen.flac.FLAC, image_data: bytes) -> None:
        """Add cover image to FLAC file as a Picture block."""
        try:
            mime_type = _detect_image_mime_type(image_data)
            picture = Picture()
            picture.data = image_data
            picture.type = 3  # Cover (front)
            picture.mime = mime_type
            picture.width = 0
            picture.height = 0
            picture.depth = 0
            picture.colors = 0
            picture.desc = "Cover"
            m.add_picture(picture)
            logger.debug("Added cover image to FLAC file: {} bytes", len(image_data))
        except Exception as e:
            logger.warning("Failed to add cover to FLAC file: {}", str(e))


def _detect_image_mime_type(image_data: bytes) -> str:
    """Detect MIME type from image binary data."""
    if image_data.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if image_data.startswith(b"\x89PNG"):
        return "image/png"
    if image_data.startswith(b"GIF"):
        return "image/gif"
    if image_data.startswith(b"RIFF") and b"WEBP" in image_data[:12]:
        return "image/webp"
    logger.debug("Unknown image format, defaulting to image/jpeg")
    return "image/jpeg"
