from dataclasses import dataclass
from pathlib import Path

import mutagen
import mutagen.flac
import mutagen.mp3
import mutagen.mp4
from loguru import logger
from mutagen.id3 import TALB, TDRC, TIT2, TLEN, TPE1, TSRC
from tidalapi import Track
from tidalapi.artist import Artist, Role

from src.exceptions import MetadataError


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
    length: int = 0
    # genre: str = ""

    # Musical data
    # bpm: int = 0
    # key: str = ""
    # key_scale: str = ""

    # Additional info
    # version: str = ""

    # Audio quality metadata
    # album_replay_gain: float = 1.0
    # album_peak_amplitude: float = 0.0
    # track_replay_gain: float = 1.0
    # track_peak_amplitude: float = 0.0
    # replay_gain_write: bool = False

    # File related data
    # path_cover: str = ""
    # cover_data: bytes = b""
    # m: mutagen.mp4.MP4 | mutagen.flac.FLAC
    # media_tags: list[str]

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
        return cls(
            title=track.name if track.name else "",
            artists=cls._name_builder_artists(track),
            album=track.album.name if track.album and track.album.name else "",
            isrc=track.isrc if track.isrc else "",
            length=track.duration if track.duration else 0,
            date=track.album.available_release_date.strftime("%Y-%m-%d")
            if track.album and track.album.available_release_date
            else "",
            year=track.album.year if track.album and track.album.year else "",
            bpm=getattr(track, "bpm", 0),
            # from https://docs.mp3tag.de/mapping/ :
            # Description
            # Genre
            # Initialkey
        )

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
        self.m = mutagen.File(path_file)  # ✅ Use mutagen.File() correctly

    def write_metadata(self, track_metadata: TrackMetaData) -> bool:
        """Write metadata to the audio file."""
        if not self.m:
            logger.error("Could not load file: {}", self.path_file)
            return False

        if not self.m.tags:
            self.m.add_tags()

        if isinstance(self.m, mutagen.flac.FLAC):
            self._write_flac_tags(track_metadata)
        elif isinstance(self.m, mutagen.mp4.MP4):
            self._write_mp4_tags(track_metadata)
        elif isinstance(self.m, mutagen.mp3.MP3):
            self._write_mp3_tags(track_metadata)
        else:
            logger.warning("Unsupported file type for metadata writing: {}", self.path_file.suffix)
            return False

        try:
            self.m.save()
        except MetadataError:
            logger.error("Failed to save metadata to {}: {}", self.path_file)
            return False
        else:
            logger.info("Metadata written to file: {}", self.path_file)
            return True

    def _write_flac_tags(self, track_metadata: TrackMetaData) -> None:
        """Write FLAC tags."""
        self.m.tags["TITLE"] = track_metadata.title
        self.m.tags["ALBUM"] = track_metadata.album
        self.m.tags["ARTIST"] = track_metadata.artists
        self.m.tags["LENGTH"] = track_metadata.length
        self.m.tags["DATE"] = track_metadata.date
        self.m.tags["ISRC"] = track_metadata.isrc

    def _write_mp4_tags(self, track_metadata: TrackMetaData) -> None:
        """Write MP4 tags."""
        self.m.tags["\xa9nam"] = track_metadata.title
        self.m.tags["\xa9alb"] = track_metadata.album
        self.m.tags["\xa9ART"] = track_metadata.artists
        self.m.tags["\xa9len"] = track_metadata.length
        self.m.tags["\xa9day"] = track_metadata.date
        self.m.tags["isrc"] = track_metadata.isrc

    def _write_mp3_tags(self, track_metadata: TrackMetaData) -> None:
        """Write MP3 tags."""
        # ID3 Frame (tags) overview: https://exiftool.org/TagNames/ID3.html / https://id3.org/id3v2.3.0
        # Mapping overview: https://docs.mp3tag.de/mapping/
        self.m.tags.add(TIT2(encoding=3, text=track_metadata.title))
        self.m.tags.add(TALB(encoding=3, text=track_metadata.album))
        self.m.tags.add(TPE1(encoding=3, text=track_metadata.artists))
        self.m.tags.add(TLEN(encoding=3, text=track_metadata.length * 1000))
        self.m.tags.add(TDRC(encoding=3, text=track_metadata.date))
        self.m.tags.add(TSRC(encoding=3, text=track_metadata.isrc))

    def cleanup_tags(self) -> None:
        """Clean up any temporary tags or data."""
