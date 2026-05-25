"""MP4/M4A metadata writer."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from loguru import logger
from mutagen.mp4 import AtomDataType, MP4Cover, MP4FreeForm, MP4Tags

if TYPE_CHECKING:
    import mutagen.mp4

    from src.track_metadata import TrackMetaData


class Mp4Writer:
    """Write metadata tags and cover image to MP4/M4A files."""

    def write_tags(self, m: mutagen.mp4.MP4, metadata: TrackMetaData) -> None:
        """Write MP4 atom tags."""
        if m.tags is None:
            m.add_tags()
        tags = cast("MP4Tags", m.tags)
        tags["\xa9nam"] = [metadata.title]
        tags["\xa9alb"] = [metadata.album]
        tags["\xa9art"] = [metadata.artists]
        tags["\xa9day"] = [str(metadata.year)]

        if metadata.isrc:
            isrc_atom = MP4FreeForm(metadata.isrc.encode("utf-8"), dataformat=AtomDataType.UTF8)
            tags["----:com.apple.iTunes:ISRC"] = [isrc_atom]

        if metadata.bpm:
            tags["tmpo"] = [metadata.bpm]

        written_tags = "\n".join(f"{key}: {value}" for key, value in tags.items())
        logger.debug("MP4 tags written successfully: \n{}", written_tags)

    def add_cover(self, m: mutagen.mp4.MP4, image_data: bytes) -> None:
        """Add cover image to MP4 file."""
        if m.tags is None:
            m.add_tags()
        tags = cast("MP4Tags", m.tags)
        if image_data.startswith(b"\xff\xd8"):
            cover_format = MP4Cover.FORMAT_JPEG
        elif image_data.startswith(b"\x89PNG"):
            cover_format = MP4Cover.FORMAT_PNG
        else:
            cover_format = MP4Cover.FORMAT_JPEG
            logger.debug("Unknown image format, defaulting to JPEG")

        cover = MP4Cover(image_data, imageformat=cover_format)
        tags["covr"] = [cover]
        logger.debug("Added cover image to MP4 file: {} bytes", len(image_data))
