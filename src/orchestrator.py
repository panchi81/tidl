"""Download orchestration using streamable pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Self

from httpx import Client, Limits
from loguru import logger
from mutagen._util import MutagenError
from streamable import stream
from tidalapi.media import AudioExtensions, Track

from src.db import DownloadDB
from src.downloader import DashStreamStrategy, StandardStreamStrategy
from src.exceptions import StreamInfoError, TidlError
from src.file_manager import FileManager
from src.metadata.base import MetadataWriter
from src.postprocessor import PostProcessor
from src.services import PlaylistService, TrackService
from src.track_metadata import TrackMetaData

if TYPE_CHECKING:
    from src.client import TidlClient
    from src.stream_info import StreamInfo


@dataclass
class DownloadConfig:
    """Configuration for the download pipeline."""

    download_dir: Path = Path("./downloads")
    skip_existing: bool = False
    skip_db: bool = False
    concurrent_downloads: int = 5
    requests_per_second: int = 4
    api_delay: float = 0.5


@dataclass
class TrackResult:
    """Result of processing a single track."""

    track_name: str
    success: bool
    skipped: bool = False
    reason: str = ""


class DownloadOrchestrator:
    """Orchestrate playlist downloads using streamable pipelines."""

    def __init__(self, client: TidlClient, config: DownloadConfig | None = None) -> None:
        self.client = client
        self.config = config or DownloadConfig()

        # Services
        self.track_service = TrackService(client.session)
        self.file_manager = FileManager(self.config.download_dir, skip_existing=self.config.skip_existing)
        self.postprocessor = PostProcessor()

        # HTTP client with connection pooling
        self.http_client = Client(
            timeout=30.0,
            limits=Limits(
                max_connections=self.config.concurrent_downloads * 2,
                max_keepalive_connections=self.config.concurrent_downloads,
            ),
        )

        # Download strategies
        self.standard_strategy = StandardStreamStrategy(self.http_client)
        self.dash_strategy = DashStreamStrategy(self.http_client, self.postprocessor)

        # Database (optional)
        self.db: DownloadDB | None = None
        if not self.config.skip_db:
            self.db = DownloadDB()
            logger.info("Database integration enabled")

        # Stream info cache
        self._stream_cache: dict[str, StreamInfo] = {}

    def download_playlist(self, playlist_id: str) -> list[TrackResult]:
        """Download all tracks from a playlist using a streamable pipeline.

        Returns:
            List of TrackResult for each track processed.

        """
        playlist_name, tracks = self._resolve_playlist(playlist_id)
        self.file_manager.download_dir = self.config.download_dir / playlist_name

        logger.info(
            "Downloading {} tracks from '{}' ({} req/s, {} concurrent)",
            len(tracks),
            playlist_name,
            self.config.requests_per_second,
            self.config.concurrent_downloads,
        )

        results: list[TrackResult] = list(
            stream(tracks)
            .throttle(self.config.requests_per_second, per=timedelta(seconds=1))
            .map(self._process_track, concurrency=self.config.concurrent_downloads)
            .catch(TidlError, replace=lambda e: TrackResult(track_name="unknown", success=False, reason=str(e)))
            .observe("downloading")
        )

        successful = sum(1 for r in results if r.success)
        logger.info("Downloaded {}/{} tracks successfully", successful, len(results))
        return results

    def _resolve_playlist(self, playlist_id: str) -> tuple[str, list[Track]]:
        """Fetch playlist and its tracks."""
        playlist_service = PlaylistService(self.client.session)
        playlist = playlist_service.get_playlist(playlist_id)
        tracks = playlist_service.get_playlist_tracks(playlist)
        logger.info("Found playlist: '{}' with {} tracks", playlist.name, len(tracks))
        return playlist.name, tracks

    def _process_track(self, track: Track) -> TrackResult:
        """Process a single track: validate, check cache/DB, download, post-process, finalize."""
        # Validate
        if not track.available or track.duration <= 0:
            return TrackResult(track_name=track.full_name, success=False, reason="Track unavailable or zero duration")

        # Get stream info
        try:
            stream_info = self._get_stream_info(track)
        except StreamInfoError as e:
            return TrackResult(track_name=track.full_name, success=False, reason=str(e))

        # Check DB for existing download / quality upgrade
        if self.db and not self.config.skip_db:
            skip_result = self._check_db_skip(track, stream_info)
            if skip_result is not None:
                return skip_result

        # Check if file exists on disk
        safe_name = self.track_service.get_track_safe_name(track)
        final_path, should_skip = self.file_manager.check_if_exists(safe_name, stream_info.file_extension_atm)

        if should_skip:
            self._maybe_record_existing(track, final_path, stream_info)
            return TrackResult(track_name=track.full_name, success=True, skipped=True, reason="File exists on disk")

        # Download, post-process, finalize
        return self._download_and_finalize(track, stream_info, final_path)

    def _download_and_finalize(self, track: Track, stream_info: StreamInfo, final_path: Path) -> TrackResult:
        """Download, post-process, write metadata, and record in DB."""
        try:
            with FileManager.workspace(track.name) as workspace:
                downloaded_file = self._download(stream_info, track, workspace)
                if not downloaded_file:
                    return TrackResult(track_name=track.full_name, success=False, reason="Download failed")

                # Post-process
                processed_file = self.postprocessor.process(downloaded_file, stream_info)
                if not processed_file:
                    return TrackResult(track_name=track.full_name, success=False, reason="Post-processing failed")

                # Finalize (move to final location)
                target_path = self.file_manager.finalize(processed_file, final_path)

                # Write metadata
                if target_path.suffix in (AudioExtensions.FLAC, AudioExtensions.M4A, AudioExtensions.MP4):
                    self._write_metadata(target_path, track)

                # Record in DB
                self._record_download(track, target_path, stream_info)

                return TrackResult(track_name=track.full_name, success=True)

        except (FileNotFoundError, OSError) as e:
            return TrackResult(track_name=track.full_name, success=False, reason=str(e))
        except (StreamInfoError, TidlError):
            logger.exception("Failed to download track: {}", track.full_name)
            return TrackResult(track_name=track.full_name, success=False, reason="Unexpected error")

    def _get_stream_info(self, track: Track) -> StreamInfo:
        """Get stream info with caching."""
        cache_key = str(track.id)
        if cache_key not in self._stream_cache:
            self._stream_cache[cache_key] = self.track_service.get_stream_info(track, self.client)
        return self._stream_cache[cache_key]

    def _download(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Select and execute the appropriate download strategy."""
        if stream_info.is_dash_stream:
            return self.dash_strategy.download(stream_info, track, workspace)
        return self.standard_strategy.download(stream_info, track, workspace)

    def _check_db_skip(self, track: Track, stream_info: StreamInfo) -> TrackResult | None:
        """Check database for existing download. Returns TrackResult if should skip, None otherwise."""
        if not self.db or not self.db.is_track_downloaded(track):
            return None

        new_quality = stream_info.quality.name
        if not self.db.should_upgrade_quality(track, new_quality):
            return TrackResult(
                track_name=track.full_name, success=True, skipped=True, reason="Already in DB at same/better quality"
            )

        existing = self.db.get_best_quality_downloaded(track)
        logger.info("Quality upgrade available for {}: {} -> {}", track.full_name, existing, new_quality)
        return None  # Proceed with download

    def _maybe_record_existing(self, track: Track, final_path: Path, stream_info: StreamInfo) -> None:
        """Add an existing file to DB if not already tracked."""
        if not self.db or self.db.is_track_downloaded(track):
            return
        try:
            file_size = final_path.stat().st_size if final_path.exists() else None
            self.db.mark_track_downloaded(
                track=track,
                file_path=str(final_path),
                file_size=file_size,
                quality=stream_info.quality.name,
                has_metadata=True,
            )
        except OSError:
            logger.exception("Failed to add existing file to DB: {}", track.full_name)

    def _record_download(self, track: Track, file_path: Path, stream_info: StreamInfo) -> None:
        """Record successful download in DB."""
        if not self.db:
            return
        try:
            file_size = file_path.stat().st_size if file_path.exists() else None
            self.db.mark_track_downloaded(
                track=track,
                file_path=str(file_path),
                file_size=file_size,
                quality=stream_info.quality.name,
                has_metadata=True,
            )
        except OSError:
            logger.exception("Failed to record download in DB: {}", track.full_name)

    def _write_metadata(self, file_path: Path, track: Track) -> None:
        """Write metadata to the finalized audio file."""
        try:
            metadata = TrackMetaData.from_track(track)
            writer = MetadataWriter(file_path)
            writer.write(metadata)
            logger.debug("Metadata written to: {}", file_path.name)
        except (MutagenError, OSError):
            logger.exception("Failed to write metadata to {}", file_path.name)

    def close(self) -> None:
        """Clean up resources."""
        self.http_client.close()

    def __enter__(self) -> Self:
        """Enter the context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Exit the context manager and clean up resources."""
        self.close()
