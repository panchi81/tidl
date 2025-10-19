from dataclasses import dataclass

from tidalapi.media import AudioExtensions, Codec, Quality, Stream, StreamManifest, Track


@dataclass
class StreamInfo:
    """Wrapper around tidalapi Stream and StreamManifest."""

    stream: Stream
    manifest: StreamManifest

    @property
    def urls(self) -> list[str]:
        """Get URLs, single or DASH."""
        return self.manifest.get_urls()

    @property
    def metadata_tags(self) -> list[str]:
        """Get metadata tags from the stream."""
        return getattr(self.stream, "media_metadata_tags", [])

    @property
    def quality(self) -> Quality:
        return Quality(self.stream.audio_quality)

    @property
    def codec(self) -> Codec:
        return Codec(self.manifest.get_codecs())

    @property
    def file_extension_atm(self) -> str:
        """Get file extension according to the manifest."""
        return self.manifest.file_extension

    @property
    def needs_flac_extraction(self) -> bool:
        """Check if FLAC extraction is needed."""
        return self.codec.upper() == Codec.FLAC and self.file_extension_atm != AudioExtensions.FLAC

    @property
    def predicted_file_extension(self) -> str:
        """Predict file extension based on quality and type."""
        # MPEG-4 is simply a container format for different audio / video encoded lines, like FLAC, AAC, M4A etc.
        # '*.m4a' is usually used as file extension, if the container contains only audio lines
        # See https://en.wikipedia.org/wiki/MP4_file_format
        return (
            AudioExtensions.FLAC
            if (self.needs_flac_extraction and self.quality in (Quality.hi_res_lossless, Quality.high_lossless))
            or ("HIRES_LOSSLESS" not in self.metadata_tags and self.quality not in (Quality.low_96k, Quality.low_320k))
            or self.quality == Quality.high_lossless
            else AudioExtensions.M4A
        )

    @property
    def mime_type(self) -> str:
        """Get MIME type."""
        return self.manifest.mime_type

    @property
    def is_mqa(self) -> bool:
        """Check for MQA metadata tags."""
        return "MQA" in self.metadata_tags or self.manifest.codecs.upper() == "MQA"

    @property
    def is_encrypted(self) -> bool:
        """Check if stream is encrypted."""
        return self.manifest.is_encrypted

    @property
    def encryption_key(self) -> str | None:
        """Get encryption key if available."""
        return self.manifest.encryption_key

    @property
    def is_dash_stream(self) -> bool:
        """Check if this is a DASH stream using tidalapi's detection."""
        return self.stream.is_mpd and len(self.urls) > 1

    @property
    def is_single_file(self) -> bool:
        """Check if this is a single file download."""
        return not self.is_dash_stream

    @property
    def replay_gain_data(self) -> dict:
        """Get replay gain data from stream."""
        return {
            "album_replay_gain": self.stream.album_replay_gain,
            "album_peak_amplitude": self.stream.album_peak_amplitude,
            "track_replay_gain": self.stream.track_replay_gain,
            "track_peak_amplitude": self.stream.track_peak_amplitude,
        }

    @property
    def audio_resolution(self) -> tuple[int, int]:
        """Get bit depth and sample rate."""
        return self.stream.get_audio_resolution()

    @classmethod
    def from_track(cls, track: Track) -> "StreamInfo":
        """Create StreamInfo from a Track object."""
        stream = track.get_stream()
        manifest = stream.get_stream_manifest()
        stream.media_metadata_tags = getattr(track, "media_metadata_tags", [])

        return cls(stream=stream, manifest=manifest)
