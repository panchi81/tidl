"""MP3 metadata writer."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from loguru import logger
from mutagen.id3 import APIC, ID3, TALB, TDRC, TIT2, TLEN, TPE1, TSRC, TYER

from src.metadata.flac import _detect_image_mime_type

if TYPE_CHECKING:
    import mutagen.id3
    import mutagen.mp3

    from src.track_metadata import TrackMetaData


class Mp3Writer:
    """Write metadata tags and cover image to MP3 files (ID3)."""

    def write_tags(self, m: mutagen.id3.ID3 | mutagen.mp3.MP3, metadata: TrackMetaData) -> None:
        """Write ID3 frames to MP3 file."""
        tags = cast("ID3", m.tags if hasattr(m, "tags") and m.tags else m)
        tags.add(TIT2(encoding=3, text=metadata.title))
        tags.add(TALB(encoding=3, text=metadata.album))
        tags.add(TPE1(encoding=3, text=metadata.artists))
        tags.add(TLEN(encoding=3, text=str(metadata.length * 1000)))
        tags.add(TDRC(encoding=3, text=metadata.date))
        tags.add(TYER(encoding=3, text=str(metadata.year)))
        if metadata.isrc:
            tags.add(TSRC(encoding=3, text=metadata.isrc))

    def add_cover(self, m: mutagen.id3.ID3 | mutagen.mp3.MP3, image_data: bytes) -> None:
        """Add APIC frame (cover image) to MP3 file."""
        mime_type = _detect_image_mime_type(image_data)
        tags = cast("ID3", m.tags if hasattr(m, "tags") and m.tags else m)
        cover_frame = APIC(
            encoding=3,
            mime=mime_type,
            type=3,  # Cover (front)
            desc="Cover",
            data=image_data,
        )
        tags.add(cover_frame)
        logger.debug("Added cover image to MP3 file: {} bytes", len(image_data))
