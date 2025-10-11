from dataclasses import dataclass

from tidalapi.media import Codec, Quality, Stream, StreamManifest, Track


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
    def quality(self) -> Quality:
        return Quality(self.stream.audio_quality)

    @property
    def codec(self) -> Codec:
        return Codec(self.manifest.get_codecs())

    @property
    def file_extension(self) -> str:
        """Get file extension."""
        return self.manifest.file_extension

    @property
    def mime_type(self) -> str:
        """Get MIME type."""
        return self.manifest.mime_type

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

        return cls(stream=stream, manifest=manifest)
