from asyncio import Lock, Semaphore, gather
from asyncio import sleep as async_sleep
from collections.abc import Callable, Generator
from contextlib import contextmanager
from json import loads as json_loads
from pathlib import Path
from shutil import move
from subprocess import run as subprocess_run
from tempfile import TemporaryDirectory
from time import time

from aiofiles import open as aio_open
from ffmpeg import FFmpeg
from httpx import AsyncClient, Client, HTTPError, Limits
from loguru import logger
from tidalapi.media import AudioExtensions, Track

from src.client import TidlClient
from src.db import DownloadDB
from src.decryption import decrypt_file, decrypt_security_token
from src.exceptions import StreamInfoError
from src.services import PlaylistService, TrackService
from src.stream_info import StreamInfo
from src.track_metadata import MetadataWriter, TrackMetaData


class RateLimiter:
    """Simple rate limiter to space out API calls."""

    def __init__(self, min_interval: float = 0.5) -> None:
        """Initialize rate limiter.

        Args:
            min_interval: Minimum seconds between calls

        """
        self.min_interval = min_interval
        self.last_call = 0.0

    async def wait(self) -> None:
        """Wait if needed to respect rate limit."""
        now = time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            await async_sleep(self.min_interval - elapsed)
        self.last_call = time()


class Download:
    """Manage Downloads with batch processing and rate limiting."""

    def __init__(  # noqa: PLR0913
        self,
        track_service: TrackService,
        client: TidlClient,
        download_dir: Path = Path("./downloads"),
        fn_logger: Callable = logger,
        *,
        skip_existing: bool = False,
        skip_db: bool = False,
        batch_size: int = 20,
        concurrent_downloads: int = 5,
        batch_delay: float = 15.0,
        api_delay: float = 0.5,
    ) -> None:
        """Initialize Download manager.

        Args:
            track_service: Service to handle track operations.
            client: Authenticated TIDL client.
            download_dir: Directory to save downloaded tracks.
            fn_logger: Logger function or object.
            skip_existing: Whether to skip existing files on disk.
            skip_db: Whether to skip database operations.
            batch_size: Number of tracks to process per batch.
            concurrent_downloads: Max concurrent downloads within a batch.
            batch_delay: Seconds to wait between batches.
            api_delay: Minimum seconds between API calls.

        """
        self.track_service = track_service
        self.tdl_client = client
        self.download_dir = download_dir
        self.fn_logger = fn_logger
        self.skip_existing = skip_existing
        self.skip_db = skip_db

        # Batch processing configuration
        self.batch_size = batch_size
        self.concurrent_downloads = concurrent_downloads
        self.batch_delay = batch_delay

        # Rate limiting
        self.rate_limiter = RateLimiter(min_interval=api_delay)
        self.api_lock = Lock()  # Serialize API calls to prevent bursts

        # Database integration
        if not skip_db:
            self.db = DownloadDB()
            self.fn_logger.info("Database integration enabled")

        # HTTP client with optimized connection pooling
        self.httpx_client = Client(
            timeout=30.0,
            limits=Limits(max_connections=concurrent_downloads * 2, max_keepalive_connections=concurrent_downloads),
        )

        # Stream info cache (for retries within same session)
        self._stream_cache: dict[str, StreamInfo] = {}

    async def orchestrate_download(self, playlist_id: str) -> dict[str, bool]:
        """Manage download process with batching."""
        playlist_name, tracks = self.resolve_tracks_from_playlist(playlist_id)
        self.fn_logger.info("Preparing to download {} tracks in batches of {}", len(tracks), self.batch_size)
        self.download_dir = self.download_dir / playlist_name

        results: dict[str, bool] = {}
        total_batches = (len(tracks) + self.batch_size - 1) // self.batch_size

        # Process tracks in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, len(tracks))
            batch = tracks[start_idx:end_idx]

            self.fn_logger.info("Processing batch {}/{} ({} tracks)", batch_num + 1, total_batches, len(batch))

            # Process batch with concurrency limit
            batch_results = await self._process_batch(batch)
            results.update(batch_results)

            # Delay between batches (except after last batch)
            if batch_num < total_batches - 1:
                self.fn_logger.info("Waiting {} seconds before next batch...", self.batch_delay)
                await async_sleep(self.batch_delay)

        progress = sum(v is True for v in results.values())
        self.fn_logger.info("Downloaded {}/{} tracks successfully", progress, len(tracks))
        return results

    async def _process_batch(self, tracks: list[Track]) -> dict[str, bool]:
        """Process a batch of tracks with concurrency control."""
        semaphore = Semaphore(self.concurrent_downloads)

        async def process_with_semaphore(track: Track) -> tuple[str, bool]:
            async with semaphore:
                result = await self.process_track(track)
                return track.full_name, result

        results_list = await gather(*(process_with_semaphore(track) for track in tracks), return_exceptions=True)

        return {name: result if isinstance(result, bool) else False for name, result in results_list}

    def resolve_tracks_from_playlist(self, playlist_id: str) -> tuple[str, list[Track]]:
        """Fetch tracks from a playlist by ID."""
        playlist_service = PlaylistService(self.tdl_client.session)
        playlist = playlist_service.get_playlist(playlist_id)
        tracks = playlist_service.get_playlist_tracks(playlist)
        self.fn_logger.info("Found playlist: {} with {} tracks", playlist.name, playlist.get_tracks_count())
        return playlist.name, tracks

    async def process_track(self, track: Track) -> bool:  # noqa: C901
        """Process track data."""
        # Validation
        if not self._validate_track(track):
            return False

        # Serialize API calls to prevent concurrent bursts
        async with self.api_lock:
            await self.rate_limiter.wait()
            # Get stream info (with caching)
            try:
                stream_info = self._get_cached_stream_info(track)
            except StreamInfoError:
                self.fn_logger.exception("Failed to get stream info for track: {}", track.full_name)
                return False

        # Check database for quality upgrades (if enabled)
        if not self.skip_db and self.db.is_track_downloaded(track):
            # Check if new quality is better (use .name to get enum name like "high_lossless")
            new_quality_str = stream_info.quality.name
            existing_quality = self.db.get_best_quality_downloaded(track)

            # Debug logging
            self.fn_logger.debug(
                "Quality check for {}: existing='{}' new='{}' upgrade={}",
                track.full_name,
                existing_quality,
                new_quality_str,
                self.db.should_upgrade_quality(track, new_quality_str),
            )

            if not self.db.should_upgrade_quality(track, new_quality_str):
                self.fn_logger.info("Skipping (DB) already-downloaded track: {}", track.full_name)
                return True
            # Quality upgrade available
            self.fn_logger.info(
                "Quality upgrade available for {}: {} -> {}", track.full_name, existing_quality, new_quality_str
            )
            # Continue with download to upgrade

        # Check if exists on disk
        safe_name = self.track_service.get_track_safe_name(track)
        final_path, should_skip = self._check_if_exists(safe_name, stream_info.file_extension_atm)

        if should_skip:
            self.fn_logger.info("Skipping existing file: {}", final_path.name)

            # Add to database if not already there
            if not self.skip_db and not self.db.is_track_downloaded(track):
                try:
                    file_size = final_path.stat().st_size if final_path.exists() else None
                    self.db.mark_track_downloaded(
                        track=track,
                        file_path=str(final_path),
                        file_size=file_size,
                        quality=stream_info.quality.name,
                        has_metadata=True,
                    )
                    self.fn_logger.debug("Added existing file to database: {}", final_path.name)
                except Exception:
                    self.fn_logger.exception("Failed to add existing file to DB: {}", track.full_name)

            return True

        # Download
        try:
            with self.download_workspace(track.name) as workspace:
                success = await self._process_download(stream_info, track, workspace, final_path)

                # Record in database
                if success and not self.skip_db:
                    try:
                        file_size = final_path.stat().st_size if final_path.exists() else None
                        self.db.mark_track_downloaded(
                            track=track,
                            file_path=str(final_path),
                            file_size=file_size,
                            quality=stream_info.quality.name,
                            has_metadata=True,
                        )
                    except Exception:
                        self.fn_logger.exception("Failed to mark track as downloaded in DB: {}", track.full_name)

                return success

        except Exception:
            self.fn_logger.exception("Failed to download track: {}", track.full_name)
            return False

    def _get_cached_stream_info(self, track: Track) -> StreamInfo:
        """Get stream info with caching to avoid redundant API calls."""
        cache_key = str(track.id)

        if cache_key not in self._stream_cache:
            self._stream_cache[cache_key] = self.track_service.get_stream_info(track, self.tdl_client)

        return self._stream_cache[cache_key]

    def _validate_track(self, track: Track) -> bool:
        """Validate track before download."""
        return track.available and track.duration > 0

    def _check_if_exists(self, safe_name: str, file_extension: str) -> tuple[Path, bool]:
        """Check existing files."""
        filepath = self.download_dir / f"{safe_name}{file_extension}"
        should_skip = filepath.exists() and self.skip_existing
        return filepath, should_skip

    @contextmanager
    def download_workspace(self, track_name: str) -> Generator[Path]:
        """Context manager for download workspace with cleanup."""
        safe_name = "".join(c for c in track_name if c.isalnum() or c in ("-", "_"))[:50]

        with TemporaryDirectory(prefix=f"tidl_{safe_name}_") as temp_dir:
            workspace = Path(temp_dir)
            try:
                yield workspace
            except Exception:
                self.fn_logger.exception("Download workspace error")
            finally:
                self.fn_logger.debug("Cleaning up download workspace")

    async def _process_download(self, stream_info: StreamInfo, track: Track, workspace: Path, final_path: Path) -> bool:
        """Pre-process, download, and, post-process track."""
        try:
            if stream_info.is_dash_stream:
                segment_files = await self._download_dash_stream(stream_info, track, workspace)
                merged_file = workspace / f"merged{stream_info.file_extension_atm}"
                self._merge_dash_segments(segment_files, merged_file)
                downloaded_file = merged_file
            else:
                downloaded_file = self._download_standard_stream(stream_info, track, workspace)

            if not downloaded_file or not downloaded_file.exists():
                return False

            processed_file = self._post_process_file(downloaded_file, track, stream_info)
            if not processed_file or not processed_file.exists():
                self.fn_logger.error("Post-processing failed for {}", track.full_name)
                return False

            return self._finalize_download(processed_file, final_path, track)

        except Exception:
            self.fn_logger.exception("Error while processing track: {}", track.full_name)
            return False

    def _download_standard_stream(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download single file."""
        temp_file = workspace / f"download{stream_info.file_extension_atm}"
        url = stream_info.urls[0]

        return self._download_stream(url, temp_file, track.name)

    async def _download_dash_stream(self, stream_info: StreamInfo, track: Track, workspace: Path) -> list[Path]:
        """Download DASH segments."""
        segments_dir = workspace / "segments"
        segments_dir.mkdir(exist_ok=True)

        tasks = []
        segment_files = []
        for url in stream_info.urls:
            url_filename = url.split("/")[-1].split("?")[0]
            filename_stem = url_filename.split("_")[-1].split(".")[0]
            segment_id = int(filename_stem) if filename_stem.isdecimal() else 0

            file_extension = (
                AudioExtensions.FLAC if stream_info.needs_flac_extraction else stream_info.file_extension_atm
            )
            segment_file = segments_dir / f"segment_{segment_id:03d}{file_extension}"
            tasks.append(self.download_stream(url, segment_file, track.name))
            segment_files.append(segment_file)

        await gather(*tasks, return_exceptions=True)

        if stream_info.is_encrypted and stream_info.encryption_key:
            key, nonce = decrypt_security_token(stream_info.encryption_key)
            decrypted_files = []
            for seg_file in segment_files:
                decrypted_file = seg_file.with_suffix(".decrypted")
                decrypt_file(seg_file, decrypted_file, key, nonce)
                decrypted_files.append(decrypted_file)
            segment_files = decrypted_files

        return segment_files

    async def download_stream(self: "Download", url: str, filepath: Path, description: str) -> Path | None:
        """Download file asynchronously."""
        async with AsyncClient() as client:
            try:
                response = await client.get(url, timeout=30.0)
                response.raise_for_status()

                async with aio_open(filepath, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        await f.write(chunk)

            except HTTPError:
                self.fn_logger.exception("Failed to download {}", description)
                if filepath.exists():
                    filepath.unlink()
                return None

            except Exception:
                self.fn_logger.exception("Unexpected error during download of {}", description)
                if filepath.exists():
                    filepath.unlink()
                return None
            else:
                return filepath

    def _download_stream(self, url: str, filepath: Path, description: str) -> Path | None:
        """Download file synchronously (fallback)."""
        try:
            response = self.httpx_client.get(url)
            response.raise_for_status()

            with filepath.open("wb") as f:
                f.write(response.content)

        except HTTPError:
            self.fn_logger.exception("Failed to download {}", description)
            if filepath.exists():
                filepath.unlink()
            return None

        except Exception:
            self.fn_logger.exception("Unexpected error during download of {}", description)
            if filepath.exists():
                filepath.unlink()
            return None

        else:
            return filepath

    def _merge_dash_segments(self, segment_files: list[Path], output_file: Path) -> None:
        """Merge DASH segments into a single file."""
        try:

            def get_segment_id(path: Path) -> int:
                try:
                    return int(path.stem.split("_")[-1])
                except (ValueError, IndexError):
                    return 0

            sorted_segments = sorted(segment_files, key=get_segment_id)
            chunk_size = 4 * 1024 * 1024  # 4 MB

            with output_file.open("wb") as f_target:
                for i, segment_file in enumerate(sorted_segments, start=1):
                    if not segment_file.exists():
                        self.fn_logger.error("Segment file missing: {}", segment_file.name)

                    self.fn_logger.debug("Merging segment {}/{}: {}", i, len(sorted_segments), segment_file.name)

                    with segment_file.open("rb") as f_segment:
                        while chunk := f_segment.read(chunk_size):
                            f_target.write(chunk)

            self.fn_logger.debug("Successfully merged {} segments into {}", len(sorted_segments), output_file)

        except Exception:
            self.fn_logger.exception("Error during binary segment merging")
            raise

    def _post_process_file(self, temp_file: Path, track: Track, stream_info: StreamInfo) -> Path | None:
        """Post-process downloaded file."""
        try:
            # Decrypt if needed (skip for DASH files as they're already decrypted)
            if stream_info.is_encrypted and not stream_info.is_dash_stream:
                self.fn_logger.debug("Decrypting file {}", temp_file.name)

                if not stream_info.encryption_key:
                    self.fn_logger.error("Missing encryption key for {}", track.full_name)
                    return None

                key, nonce = decrypt_security_token(stream_info.encryption_key)
                decrypted_file = temp_file.with_suffix(".decrypted")
                decrypt_file(temp_file, decrypted_file, key, nonce)
                temp_file = decrypted_file

            # Extract FLAC from MP4 if needed
            codec, container = self._probe_codec_and_container(temp_file)

            if stream_info.file_extension_atm != stream_info.predicted_file_extension:
                self.fn_logger.warning(
                    "Manifest extension ({}) does not match predicted extension ({}).",
                    stream_info.file_extension_atm,
                    stream_info.predicted_file_extension,
                )

            if codec not in ("aac", "flac", "alac"):
                logger.warning(f"Unexpected codec detected: {codec} in container {container}. File: {temp_file}")

            if stream_info.needs_flac_extraction and codec == "flac" and "mp4" in container:
                self.fn_logger.warning("FLAC audio in MP4 container detected. Extracting to separate FLAC file.")
                extracted_file = self._extract_flac(temp_file)
                if extracted_file.exists():
                    return extracted_file

                self.fn_logger.error("FLAC extraction failed for {}", track.full_name)
                return None

            if temp_file.exists():
                return temp_file
            self.fn_logger.error("File missing {}", temp_file)
            return None  # noqa: TRY300

        except Exception:
            self.fn_logger.exception("Post-processing failed for {}", track.full_name)
            return None

    def _probe_codec_and_container(self, file_path: Path) -> tuple[str, str]:
        """Ffprobe the file to get codec and container information."""
        try:
            probe_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "format=format_name:stream=codec_name",
                "-of",
                "json",
                str(file_path),
            ]
            result = subprocess_run(probe_cmd, capture_output=True, text=True, timeout=10, check=False)  # noqa: S603
            if result.returncode == 0:
                info = json_loads(result.stdout)
                codec = info["streams"][0]["codec_name"] if info.get("streams") else ""
                container = info["format"]["format_name"] if info.get("format") else ""
                return codec.lower(), container.lower()
        except Exception:
            self.fn_logger.exception("ffprobe failed for {}", file_path.name)
        return "", ""

    def _extract_flac(self, mp4_file: Path) -> Path:
        """Extract FLAC audio from MP4 container."""
        output_flac_file = mp4_file.with_suffix(AudioExtensions.FLAC)
        ffmpeg = (
            FFmpeg()
            .input(url=str(mp4_file))
            .output(
                url=output_flac_file,
                map=0,
                movflags="use_metadata_tags",
                acodec="copy",
                map_metadata="0:g",
                loglevel="quiet",
            )
        )
        ffmpeg.execute()
        if not output_flac_file.exists():
            self.fn_logger.error("FFmpeg failed to create FLAC file: {}", output_flac_file.name)
        else:
            self.fn_logger.debug("Extracted FLAC file: {}", output_flac_file.name)

        if mp4_file.exists():
            mp4_file.unlink()

        return output_flac_file

    def _finalize_download(self, processed_file: Path, final_path: Path, track: Track) -> bool:
        """Finalize the download by moving the temp file to its final location, renaming, and adding metadata."""
        try:
            if not processed_file.exists():
                self.fn_logger.error("File does not exist and cannot be finalized: {}", processed_file)
                return False

            target_path = final_path.with_suffix(processed_file.suffix)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            move(str(processed_file), str(target_path))
            self.fn_logger.info("Moved {} to {}", processed_file.name, target_path.name)

            if target_path.suffix in (AudioExtensions.FLAC, AudioExtensions.M4A, AudioExtensions.MP4):
                self._add_metadata(target_path, track)
                self.fn_logger.debug("Added metadata to {}", target_path.name)
            else:
                self.fn_logger.debug("Skipping metadata for {}", target_path.name)

        except Exception:
            self.fn_logger.exception("Failed to finalize download for {}", track.name)
            return False

        else:
            self.fn_logger.info("Finalized download for {}", track.name)
            return True

    def _add_metadata(self, file_path: Path, track: Track) -> None:
        """Add metadata to the audio file."""
        try:
            track_metadata = TrackMetaData.from_track(track)
            writer = MetadataWriter(file_path)
            writer.write_metadata(track_metadata)
            self.fn_logger.debug("Metadata added to: {}", file_path.name)

        except Exception:
            self.fn_logger.exception("Failed to add metadata to {}", file_path.name)
