"""Metadata writing package — strategy pattern for audio format tagging."""

from src.metadata.base import MetadataWriter
from src.metadata.flac import FlacWriter
from src.metadata.mp3 import Mp3Writer
from src.metadata.mp4 import Mp4Writer

__all__ = ["FlacWriter", "MetadataWriter", "Mp3Writer", "Mp4Writer"]
