"""MP4/M4A metadata writer."""

from __future__ import annotations

import mutagen
import mutagen.mp4
from loguru import logger
from mutagen.mp4 import AtomDataType, MP4Cover, MP4FreeForm

from src.track_metadata import TrackMetaData


class Mp4Writer:
    """Write metadata tags and cover image to MP4/M4A files."""

    def write_tags(self, m: mutagen.mp4.MP4, metadata: TrackMetaData) -> None:
        """Write MP4 atom tags."""
        try:
            m.tags["\xa9nam"] = [metadata.title]
            m.tags["\xa9alb"] = [metadata.album]
            m.tags["\xa9art"] = [metadata.artists]
            m.tags["\xa9day"] = [str(metadata.year)]

            if metadata.isrc:
                isrc_atom = MP4FreeForm(metadata.isrc.encode("utf-8"), dataformat=AtomDataType.UTF8)
                m.tags["----:com.apple.iTunes:ISRC"] = [isrc_atom]

            if metadata.bpm:
                m.tags["tmpo"] = [metadata.bpm]

            written_tags = "\n".join(f"{key}: {value}" for key, value in m.tags.items())
            logger.debug("MP4 tags written successfully: \n{}", written_tags)

        except Exception as e:
            logger.error("Error writing MP4 tags: {}", str(e))
            raise

    def add_cover(self, m: mutagen.mp4.MP4, image_data: bytes) -> None:
        """Add cover image to MP4 file."""
        try:
            if image_data.startswith(b"\xff\xd8"):
                cover_format = MP4Cover.FORMAT_JPEG
            elif image_data.startswith(b"\x89PNG"):
                cover_format = MP4Cover.FORMAT_PNG
            else:
                cover_format = MP4Cover.FORMAT_JPEG
                logger.debug("Unknown image format, defaulting to JPEG")

            cover = MP4Cover(image_data, imageformat=cover_format)
            m.tags["covr"] = [cover]
            logger.debug("Added cover image to MP4 file: {} bytes", len(image_data))

        except Exception as e:
            logger.warning("Failed to add cover to MP4 file: {}", str(e))
            raise
