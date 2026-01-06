from dataclasses import dataclass
from pathlib import Path

import httpx
import mutagen
import mutagen.flac
import mutagen.mp3
import mutagen.mp4
from loguru import logger
from mutagen.flac import Picture
from mutagen.id3 import APIC, ID3, TALB, TDRC, TIT2, TLEN, TPE1, TSRC, TYER
from mutagen.mp4 import AtomDataType, MP4Cover, MP4FreeForm
from tidalapi import Track
from tidalapi.artist import Artist, Role

from src.exceptions import CoverImageError

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
    # genre: str = ""

    # Musical data
    bpm: int = 0
    key: str = ""
    key_scale: str = ""

    # Additional info
    # version: str | None = None

    # Audio quality metadata
    # album_replay_gain: float = 1.0
    # album_peak_amplitude: float = 0.0
    # track_replay_gain: float = 1.0
    # track_peak_amplitude: float = 0.0
    # replay_gain_write: bool = False

    # File related data
    cover: bytes = b""
    # m: mutagen.mp4.MP4 | mutagen.flac.FLAC
    # media_tags: list[str]

    # Performance metadata flags
    # stem_ready: bool = False
    # dj_ready: bool = False

    @property
    def full_title(self) -> str:
        """Return artist - title format."""
        return f"{self.artists} - {self.title}"

    @property
    def is_hi_res(self) -> bool:
        """Check if the track is hi-res quality."""
        # For now, we'll return False since quality isn't stored in this class
        # This should be implemented when quality information is added to TrackMetaData
        return False

    @classmethod
    def from_track(cls, track: Track) -> "TrackMetaData":
        """Create TrackMetaData."""
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
            # from https://docs.mp3tag.de/mapping/ :
            # BPM
            # Description
            # Genre
            # Initialkey
            bpm=track.bpm or 0,
            key=track.key or "",
            key_scale=track.key_scale or "",
            # peak=track.peak if track.peak else 0.0,
            # replay_gain=track.replay_gain if track.replay_gain else 1.0,
        )

    @classmethod
    def _download_cover_image(cls, track: Track) -> bytes:
        """Download cover image.

        Tries to download in this order:
        1. 1280x1280
        2. 640x640
        3. 320x320

        Args:
            track: Track object

        Returns:
            bytes: Image data, or empty bytes if download fails

        """
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
                        logger.debug("Successfully downloaded cover image: {} bytes ({}px)", len(image_data), quality)
                        return image_data
                    logger.warning("Invalid image content type: {}", response.headers.get("content-type"))
            except Exception as e:
                logger.warning("Error downloading cover at {} quality: {}", quality, str(e))
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


class MetadataWriter:
    """Write metadata to audio files using mutagen."""

    def __init__(self, path_file: Path) -> None:
        self.path_file = path_file
        self.m = self._load_file_with_format_detection()

    def _load_file_with_format_detection(self) -> mutagen.FileType | None:
        """Load file with custom format detection for TIDAL DASH streams."""
        file_ext = self.path_file.suffix.lower()

        try:
            # Check file header to determine actual format for common extensions
            with Path(self.path_file).open("rb") as f:
                header = f.read(16)

            # Check if MP4 container (ftyp signature)
            if b"ftyp" in header:
                # This is an MP4 file with .flac extension (DASH)
                logger.debug("Detected MP4 container (file ext: {})", file_ext)
                return mutagen.mp4.MP4(str(self.path_file))

            # Check if FLAC file
            if header.startswith(b"fLaC"):
                # This is a real FLAC file
                logger.debug("Detected FLAC file (file ext: {})", file_ext)
                return mutagen.flac.FLAC(str(self.path_file))

            # TODO: Check for AAC in MP4 container...

            # Check if MP3 file (ID3 tag or MPEG frame sync)
            if header.startswith((b"ID3", b"\xff\xfb", b"\xff\xf3")):
                logger.debug("Detected MP3 file (file ext: {})", file_ext)
                # return mutagen.mp3.MP3(str(self.path_file))
                return ID3(str(self.path_file))

            # Fall back to standard mutagen detection for other formats
            logger.debug("Using standard mutagen detection for {} (header: {})", file_ext, header[:8].hex())
            return mutagen.File(str(self.path_file))

        except Exception as e:
            logger.error("Failed to load file {}: {}", self.path_file, e)
            return None

    def write_metadata(self, track_metadata: TrackMetaData) -> bool:
        """Write metadata to the audio file."""
        logger.debug("write_metadata called, self.m type: {}, self.m is None: {}", type(self.m), self.m is None)
        if self.m is None:
            logger.error("Could not load file: {}", self.path_file)
            return False

        try:
            # Add tags if they don't exist
            if not self.m.tags:
                logger.debug("Adding tags to file")
                self.m.add_tags()

            # Write metadata based on detected format (like tidal_dl_ng does)
            if isinstance(self.m, mutagen.flac.FLAC):
                logger.debug("Writing FLAC tags")
                self._write_flac_tags(track_metadata)
            elif isinstance(self.m, mutagen.mp4.MP4):
                self._write_mp4_tags(track_metadata)
            elif isinstance(self.m, mutagen.mp3.MP3):
                self._write_mp3_tags(track_metadata)
            else:
                logger.warning("Unsupported file format: {}", type(self.m))
                return False

            # Add cover image if available
            if track_metadata.cover:
                self._add_cover_image(track_metadata.cover)

            self.m.save()
            logger.debug("Successfully wrote metadata to: {}", self.path_file.name)
            return True

        except Exception as e:
            logger.error("Failed to write metadata to {}: {}", self.path_file.name, e)
            return False

    def _write_flac_tags(self, track_metadata: TrackMetaData) -> None:
        """Write FLAC tags."""
        self.m.tags["TITLE"] = track_metadata.title
        self.m.tags["ALBUM"] = track_metadata.album
        self.m.tags["ARTIST"] = track_metadata.artists
        self.m.tags["LENGTH"] = str(track_metadata.length)
        self.m.tags["DATE"] = track_metadata.date
        self.m.tags["YEAR"] = str(track_metadata.year)
        if track_metadata.isrc:
            self.m.tags["ISRC"] = track_metadata.isrc
        if track_metadata.bpm:
            self.m.tags["BPM"] = str(track_metadata.bpm)

        written_tags = "\n".join(f"{key}: {value}" for key, value in self.m.tags.items())
        logger.debug("FLAC tags written successfully: \n{}", written_tags)

    def _write_mp4_tags(self, track_metadata: TrackMetaData) -> None:
        """Write MP4 tags."""
        try:
            # Write basic metadata (MP4 tags expect lists)
            self.m.tags["\xa9nam"] = [track_metadata.title]
            self.m.tags["\xa9alb"] = [track_metadata.album]
            self.m.tags["\xa9art"] = [track_metadata.artists]
            # self.m.tags["\xa9day"] = [track_metadata.date]
            self.m.tags["\xa9day"] = [str(track_metadata.year)]

            # ISRC must use iTunes freeform atom format
            if track_metadata.isrc:
                isrc_atom = MP4FreeForm(track_metadata.isrc.encode("utf-8"), dataformat=AtomDataType.UTF8)
                self.m.tags["----:com.apple.iTunes:ISRC"] = [isrc_atom]

            if track_metadata.bpm:
                self.m.tags["tmpo"] = [track_metadata.bpm]

            written_tags = "\n".join(f"{key}: {value}" for key, value in self.m.tags.items())
            logger.debug("MP4 tags written successfully: \n{}", written_tags)

        except Exception as e:
            logger.error("Error writing MP4 tags: {}", str(e))
            raise

    def _write_mp3_tags(self, track_metadata: TrackMetaData) -> None:
        """Write MP3 tags."""
        # ID3 Frame (tags) overview: https://exiftool.org/TagNames/ID3.html / https://id3.org/id3v2.3.0
        # Mapping overview: https://docs.mp3tag.de/mapping/
        self.m.tags.add(TIT2(encoding=3, text=track_metadata.title))
        self.m.tags.add(TALB(encoding=3, text=track_metadata.album))
        self.m.tags.add(TPE1(encoding=3, text=track_metadata.artists))
        self.m.tags.add(TLEN(encoding=3, text=str(track_metadata.length * 1000)))
        self.m.tags.add(TDRC(encoding=3, text=track_metadata.date))
        self.m.tags.add(TYER(encoding=3, text=str(track_metadata.year)))
        if track_metadata.isrc:
            self.m.tags.add(TSRC(encoding=3, text=track_metadata.isrc))

    def _add_cover_image(self, image_data: bytes) -> None:
        """Add cover image to file based on format (like tidal_dl_ng does)."""
        if isinstance(self.m, mutagen.flac.FLAC):
            self._add_flac_cover(image_data)
        elif isinstance(self.m, mutagen.mp4.MP4):
            self._add_mp4_cover(image_data)
        elif isinstance(self.m, mutagen.mp3.MP3):
            self._add_mp3_cover(image_data)

    def _add_flac_cover(self, image_data: bytes) -> None:
        """Add cover image to FLAC file."""
        try:
            # Detect image format from binary data
            mime_type = self._detect_image_mime_type(image_data)

            # Create Picture block
            picture = Picture()
            picture.data = image_data
            picture.type = 3  # Cover (front)
            picture.mime = mime_type
            picture.width = 0  # Let mutagen figure out dimensions
            picture.height = 0
            picture.depth = 0  # Color depth
            picture.colors = 0  # Number of colors
            picture.desc = "Cover"

            # Add picture to FLAC file
            self.m.add_picture(picture)
            logger.debug("Added cover image to FLAC file: {} bytes", len(image_data))

        except CoverImageError as e:
            logger.warning("Failed to add cover to FLAC file: {}", str(e))

    def _add_mp4_cover(self, image_data: bytes) -> None:
        """Add cover image to MP4 file."""
        try:
            # Detect format and create appropriate MP4Cover
            if image_data.startswith(b"\xff\xd8"):  # JPEG signature
                cover_format = MP4Cover.FORMAT_JPEG
            elif image_data.startswith(b"\x89PNG"):  # PNG signature
                cover_format = MP4Cover.FORMAT_PNG
            else:
                # Default to JPEG for unknown formats
                cover_format = MP4Cover.FORMAT_JPEG
                logger.debug("Unknown image format, defaulting to JPEG")

            # Create MP4Cover object
            cover = MP4Cover(image_data, imageformat=cover_format)

            # Add to MP4 tags (ensure it's a list)
            self.m.tags["covr"] = [cover]
            logger.debug("Added cover image to MP4 file: {} bytes", len(image_data))

        except Exception as e:
            logger.warning("Failed to add cover to MP4 file: {}", str(e))
            raise

    def _add_mp3_cover(self, image_data: bytes) -> None:
        """Add cover image to MP3 file."""
        try:
            # Detect MIME type
            mime_type = self._detect_image_mime_type(image_data)

            # Create APIC frame (Attached Picture)
            cover_frame = APIC(
                encoding=3,  # UTF-8
                mime=mime_type,
                type=3,  # Cover (front)
                desc="Cover",
                data=image_data,
            )

            # Add to ID3 tags
            self.m.tags.add(cover_frame)
            logger.debug("Added cover image to MP3 file: {} bytes", len(image_data))

        except CoverImageError as e:
            logger.warning("Failed to add cover to MP3 file: {}", str(e))

    def _detect_image_mime_type(self, image_data: bytes) -> str:
        """Detect MIME type from image binary data."""
        if image_data.startswith(b"\xff\xd8"):
            return "image/jpeg"
        if image_data.startswith(b"\x89PNG"):
            return "image/png"
        if image_data.startswith(b"GIF"):
            return "image/gif"
        if image_data.startswith(b"\x00\x00\x01\x00"):  # ICO format
            return "image/x-icon"
        if image_data.startswith(b"RIFF") and b"WEBP" in image_data[:12]:
            return "image/webp"

        # Default to JPEG for unknown formats
        logger.debug("Unknown image format, defaulting to image/jpeg")
        return "image/jpeg"

    def cleanup_tags(self) -> None:
        """Clean up any temporary tags or data."""
