from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from mutagen import mutagen
from mutagen.id3 import TALB, TCOM, TCOP, TDRC, TIT2, TOPE, TPE1, TRCK, TSRC, TXXX, USLT, WOAS
from tidalapi import Quality, Track
from tidalapi.artist import Artist, Role


@dataclass
class TrackMetaData:
    """Enhanced metadata wrapper for tidalapi Track."""

    # Core metadata
    title: str = ""
    artists: str = ""
    album: str = ""
    isrc: str = ""

    # Basic track info
    # albumartist: str = ""
    # tracknumber: int = 0
    date: str = ""
    # upc: str = ""
    # url_share: str = ""

    # Musical data
    bpm: int = 0
    key: str = ""
    key_scale: str = ""

    # Additional info
    # media_tags: list[str]
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

    @property
    def full_title(self) -> str:
        """Return artist - title format."""
        return f"{self.artist} - {self.name}"

    @property
    def is_hi_res(self) -> bool:
        """Check if the track is hi-res quality."""
        return self.quality in {Quality.high_lossless, Quality.hi_res_lossless}

    @classmethod
    def from_track(cls, track: Track) -> "TrackMetaData":
        return cls(
            title=track.title if track.title else track.name if track.name else "",
            artists=cls._name_builder_artists(track),
            album=track.album.name if track.album and track.album.name else "",
            isrc=track.isrc if hasattr(track, "isrc") and track.isrc else "",
            # albumartist=cls._name_builder_album_artist(track),
            # tracknumber=track.track_num,
            date=cls._get_release_date(track),
            # upc=track.album.upc if track.album and track.album.upc else "",
            # composer=track.composer if hasattr(track, "composer") and track.composer else "",
            # url_share=track.url if hasattr(track, "url") and track.url else "",
            bpm=getattr(track, "bpm", 0),
        )

    @classmethod
    def _name_builder_artists(cls, track: Track) -> str:
        """Format all artists for tags."""
        return "; ".join(artist.name for artist in track.artists)

    @classmethod
    def _name_builder_album_artist(cls, track: Track) -> str:
        """Format main album artists."""
        artists_tmp: list[str] = []
        artists: list[Artist] = track.album.artists if isinstance(track, Track) else track.artists

        artists_tmp = [artist.name for artist in artists if Role.main in artist.roles]

        return "; ".join(artists_tmp)

    @classmethod
    def _get_release_date(cls, track: Track) -> str:
        """Get the release date of the track."""
        return str(
            track.album.available_date.strftime("%Y-%m-%d")
            if track.album and track.album.available_date
            else track.album.release_date.strftime("%Y-%m-%d")
            if track.album and track.album.release_date
            else ""
        )

    # @classmethod
    # def from_tidal_track(cls, track: Track, *, include_stream_url: bool = False) -> "TrackMetaData":
    #     """Create TrackMetaData from a track object."""
    #     # Map TIDAL'S quality string to tidalapi's Quality enum
    #     quality_map = {
    #         "LOW": Quality.low_96k,
    #         "HIGH": Quality.low_320k,
    #         "LOSSLESS": Quality.high_lossless,
    #         "HI_RES_LOSSLESS": Quality.hi_res_lossless,
    #     }

    #     stream_url = None
    #     is_encrypted = False
    #     encryption_key = None
    #     if include_stream_url:
    #         try:
    #             logger.debug("Getting stream for track {}", track.id)
    #             stream = track.get_stream()

    #             if stream:
    #                 logger.debug("Got stream, getting manifest for track {}", track.id)
    #                 stream_manifest = stream.get_stream_manifest()

    #                 if stream_manifest:
    #                     logger.debug("Got manifest, getting URLs for track {}", track.id)
    #                     urls = stream_manifest.get_urls()

    #                     is_encrypted = stream_manifest.is_encrypted
    #                     encryption_key = stream_manifest.encryption_key if is_encrypted else None

    #                     if urls:
    #                         if len(urls) == 1:
    #                             # Single direct URL
    #                             stream_url = urls[0]
    #                             logger.debug("Got single URL for track {}: {}", track.id, stream_url)
    #                         else:
    #                             # Multiple DASH segments
    #                             stream_url = urls  # Store as list
    #                             logger.debug("Got {} DASH segments for track {}", len(urls), track.id)
    #                     else:
    #                         logger.warning("No URLs returned from manifest for track {}", track.id)
    #                 else:
    #                     logger.warning("No stream manifest for track {}", track.id)
    #             else:
    #                 logger.warning("No stream object for track {}", track.id)

    #         except (TrackError, Exception) as e:
    #             logger.exception("Failed to get stream URL for track %s: %s", track.id, e)


class MetadataWriter:
    """Write metadata to audio files using mutagen."""

    def __init__(self, path_file: Path) -> None:
        self.path_file = path_file
        self.m = mutagen.File(self.path_file)

    def write_metadata(self, track_metadata: TrackMetaData) -> bool:
        """Write metadata to the audio file."""
        if not self.m.tags:
            self.m.add_tags()

        match self.m:
            case isinstance(self.m, mutagen.flac.FLAC):
                self._write_flac_tags(track_metadata)
            case isinstance(self.m, mutagen.mp4.MP4):
                self._write_mp4_tags(track_metadata)
            case isinstance(self.m, mutagen.mp3.MP3):
                self._write_mp3_tags(track_metadata)
            case _:
                logger.warning("Unsupported file type for metadata writing: {}", self.path_file.suffix)
                return False

        self.m.save()
        logger.info("Metadata written to file: {}", self.path_file)
        return True

    def _write_flac_tags(self) -> None:
        """Write FLAC tags."""
        self.m.tags["TITLE"] = self.title
        self.m.tags["ALBUM"] = self.album
        self.m.tags["ALBUMARTIST"] = self.albumartist
        self.m.tags["ARTIST"] = self.artists
        self.m.tags["COPYRIGHT"] = self.copy_right
        self.m.tags["TRACKNUMBER"] = str(self.tracknumber)
        self.m.tags["TRACKTOTAL"] = str(self.totaltrack)
        self.m.tags["DISCNUMBER"] = str(self.discnumber)
        self.m.tags["DISCTOTAL"] = str(self.totaldisc)
        self.m.tags["DATE"] = self.date
        self.m.tags["COMPOSER"] = self.composer
        self.m.tags["ISRC"] = self.isrc
        self.m.tags["LYRICS"] = self.lyrics
        self.m.tags["URL"] = self.url_share
        self.m.tags["UPC"] = self.upc

        if self.replay_gain_write:
            self.m.tags["REPLAYGAIN_ALBUM_GAIN"] = str(self.album_replay_gain)
            self.m.tags["REPLAYGAIN_ALBUM_PEAK"] = str(self.album_peak_amplitude)
            self.m.tags["REPLAYGAIN_TRACK_GAIN"] = str(self.track_replay_gain)
            self.m.tags["REPLAYGAIN_TRACK_PEAK"] = str(self.track_peak_amplitude)

    def _write_mp4_tags(self) -> None:
        """Write mp4 tags."""
        self.m.tags["\xa9nam"] = self.title
        self.m.tags["\xa9alb"] = self.album
        self.m.tags["aART"] = self.albumartist
        self.m.tags["\xa9ART"] = self.artists
        self.m.tags["cprt"] = self.copy_right
        self.m.tags["trkn"] = [[self.tracknumber, self.totaltrack]]
        self.m.tags["disk"] = [[self.discnumber, self.totaldisc]]
        # self.m.tags['\xa9gen'] = self.genre
        self.m.tags["\xa9day"] = self.date
        self.m.tags["\xa9wrt"] = self.composer
        self.m.tags["\xa9lyr"] = self.lyrics
        self.m.tags["isrc"] = self.isrc
        self.m.tags["\xa9url"] = self.url_share
        self.m.tags["----:com.apple.iTunes:UPC"] = self.upc.encode("utf-8")

        if self.replay_gain_write:
            self.m.tags["----:com.apple.iTunes:REPLAYGAIN_ALBUM_GAIN"] = str(self.album_replay_gain).encode("utf-8")
            self.m.tags["----:com.apple.iTunes:REPLAYGAIN_ALBUM_PEAK"] = str(self.album_peak_amplitude).encode("utf-8")
            self.m.tags["----:com.apple.iTunes:REPLAYGAIN_TRACK_GAIN"] = str(self.track_replay_gain).encode("utf-8")
            self.m.tags["----:com.apple.iTunes:REPLAYGAIN_TRACK_PEAK"] = str(self.track_peak_amplitude).encode("utf-8")

    def _write_mp3_tags(self) -> None:
        """Write mp3 tags."""
        # ID3 Frame (tags) overview: https://exiftool.org/TagNames/ID3.html / https://id3.org/id3v2.3.0
        # Mapping overview: https://docs.mp3tag.de/mapping/
        self.m.tags.add(TIT2(encoding=3, text=self.title))
        self.m.tags.add(TALB(encoding=3, text=self.album))
        self.m.tags.add(TOPE(encoding=3, text=self.albumartist))
        self.m.tags.add(TPE1(encoding=3, text=self.artists))
        self.m.tags.add(TCOP(encoding=3, text=self.copy_right))
        self.m.tags.add(TRCK(encoding=3, text=str(self.tracknumber)))
        self.m.tags.add(TRCK(encoding=3, text=self.discnumber))
        self.m.tags.add(TDRC(encoding=3, text=self.date))
        self.m.tags.add(TCOM(encoding=3, text=self.composer))
        self.m.tags.add(TSRC(encoding=3, text=self.isrc))
        self.m.tags.add(USLT(encoding=3, lang="eng", desc="desc", text=self.lyrics))
        self.m.tags.add(WOAS(encoding=3, text=self.isrc))
        self.m.tags.add(TXXX(encoding=3, desc="UPC", text=self.upc))

        if self.replay_gain_write:
            self.m.tags.add(TXXX(encoding=3, desc="REPLAYGAIN_ALBUM_GAIN", text=str(self.album_replay_gain)))
            self.m.tags.add(TXXX(encoding=3, desc="REPLAYGAIN_ALBUM_PEAK", text=str(self.album_peak_amplitude)))
            self.m.tags.add(TXXX(encoding=3, desc="REPLAYGAIN_TRACK_GAIN", text=str(self.track_replay_gain)))
            self.m.tags.add(TXXX(encoding=3, desc="REPLAYGAIN_TRACK_PEAK", text=str(self.track_peak_amplitude)))

    def cleanup_tags(self) -> None: ...
