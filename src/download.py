import signal
from atexit import register
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from random import uniform
from tempfile import TemporaryDirectory
from threading import Event
from time import sleep

import httpx
from ffmpeg import FFmpeg
from httpx import Client
from loguru import logger
from tidalapi.media import AudioExtensions, Track
from tqdm import tqdm

from src.decryption import decrypt_file, decrypt_security_token
from src.exceptions import StreamInfoError
from src.services import TrackService
from src.stream_info import StreamInfo
from src.track_metadata import MetadataWriter, TrackMetaData


class Download:
    """Main class for managing downloads."""

    def __init__(
        self,
        track_service: TrackService,
        output_dir: Path | str = "./downloads",
        fn_logger: Callable = logger,
        download_delay_range: tuple[int, int] = (3, 6),
        *,
        skip_existing: bool = True,
    ) -> None:
        """Initialize the Downloader object and its attributes.

        Args:
            track_service: Service for track operations
            output_dir (Path | str, optional): Base path for downloads. Defaults to "./downloads".
            fn_logger (Callable, optional): Logger function for logging events. Defaults to logger.
            download_delay_range: Range for random delays between downloads
            skip_existing (bool, optional): Whether to skip downloading files that already exist. Defaults to True.

        """
        self.track_service = track_service
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.fn_logger = fn_logger
        self.download_delay_range = download_delay_range
        self.skip_existing = skip_existing

        # Graceful shutdown handling
        self.event_abort = Event()
        self.event_pause = Event()
        self.event_pause.set()

        # Setup httpx client with retries
        self.client = Client(timeout=30.0, limits=httpx.Limits(max_keepalive_connections=5, max_connections=10))

        # Setup signal handlers
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Set up graceful shutdown handling."""

        def signal_handler(signum: int, _frame: object) -> None:
            self.fn_logger.info("Received signal %s. Gracefully shutting down...", signum)
            self.event_abort.set()
            self._cleanup()

        def emergency_cleanup() -> None:
            """Emergency cleanup on process exit."""
            if not self.event_abort.is_set():
                self.fn_logger.info("Emergency cleanup on process exit...")
                self.event_abort.set()
                self._cleanup()

        # Handle common termination signals
        signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination request

        # On Unix systems, also handle additional signals
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, signal_handler)  # Terminal hangup
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, signal_handler)  # Quit signal

        # Register emergency cleanup for process exit
        register(emergency_cleanup)

    def _cleanup(self) -> None:
        """Perform cleanup operations during shutdown."""
        try:
            # Cancel any pending futures in the thread pool
            if hasattr(self, "executor") and self.executor:
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.fn_logger.debug("Thread pool executor shutdown initiated")
        except (AttributeError, RuntimeError) as e:
            self.fn_logger.warning("Error during executor cleanup: %s", e)

        try:
            # Close httpx client
            if hasattr(self, "client") and self.client:
                self.client.close()
                self.fn_logger.debug("HTTP client closed")
        except (AttributeError, RuntimeError) as e:
            self.fn_logger.warning("Error during client cleanup: %s", e)

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

    def download_track(self, track: Track) -> bool:
        """Download a single track."""
        if self.event_abort.is_set():
            return False

        # Validation
        if not self._validate_track(track):
            return False

        try:
            stream_info = self.track_service.get_stream_info(track)
        except StreamInfoError:
            self.fn_logger.exception("Failed to get stream info for track: {}", track.full_name)
            return False

        # Check if exists
        safe_name = self.track_service.get_track_safe_name(track)
        final_path, should_skip = self._check_if_exists(safe_name, stream_info.file_extension)

        if should_skip:
            self.fn_logger.info("Skipping existing file: {}", final_path.name)
            return True

        # Download
        with self.download_workspace(track.name) as workspace:
            return self._process_download(stream_info, track, workspace, final_path)

    def _process_download(self, stream_info: StreamInfo, track: Track, workspace: Path, final_path: Path) -> bool:
        if self.event_abort.is_set():
            return False

        downloaded_file = self._download_to_workspace(stream_info, track, workspace)
        if not downloaded_file or not downloaded_file.exists():
            return False

        processed_file = self._post_process(downloaded_file, track, stream_info)
        if not processed_file or not processed_file.exists():
            return False

        success = self._finalize_download(processed_file, final_path)
        if success:
            self._apply_download_delay()
        return success

    def _download_to_workspace(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download to workspace."""
        if stream_info.is_dash_stream:
            return self._download_dash_segments(stream_info, track, workspace)
        return self._download_single_file(stream_info, track, workspace)

    def _download_single_file(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download single file."""
        temp_file = workspace / f"download{stream_info.file_extension}"
        url = stream_info.urls[0]

        return self._download_file(url, temp_file, track.name)

    def _download_dash_segments(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download DASH segments."""
        segments_dir = workspace / "segments"
        segments_dir.mkdir(exist_ok=True)

        self.fn_logger.info("Downloading {} DASH segments for {}", len(stream_info.urls), track.name)

        segment_files = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            for i, url in enumerate(stream_info.urls, start=1):
                if self.event_abort.is_set():
                    break

                segment_file = segments_dir / f"segment_{i:03d}{stream_info.file_extension}"
                future = executor.submit(self._download_file, url, segment_file, f"Segment {i}/{len(stream_info.urls)}")
                futures.append((future, segment_file))

            for future, segment_file in futures:
                if self.event_abort.is_set():
                    future.cancel()
                    return None

                try:
                    if result_file := future.result(timeout=60):
                        segment_files.append(result_file)
                    else:
                        self.fn_logger.error("Failed to download segment: {}", segment_file.name)
                        return None
                except Exception:
                    self.fn_logger.exception("Segment download error")
                    return None

        if len(segment_files) != len(stream_info.urls):
            self.fn_logger.error("Not all segments were downloaded successfully")
            return None

        merged_file = workspace / f"merged{stream_info.file_extension}"
        self._merge_segments(segment_files, merged_file)

        return merged_file

    def _download_file(self, url: str, filepath: Path, description: str) -> Path | None:  # noqa: C901
        """Download file."""
        retries = 5
        for attempt in range(1, retries + 1):
            if self.event_abort.is_set():
                return None

            self.event_pause.wait()

            try:
                self.fn_logger.debug("Downloading {} (Attempt {}/5)", description, attempt)

                with self.client.stream("GET", url) as response:
                    response.raise_for_status()

                    total_size = int(response.headers.get("content-length", 0))

                    with (
                        filepath.open("wb") as f,
                        tqdm(desc=description[:30], total=total_size, unit="B", unit_scale=True, leave=False) as pbar,
                    ):
                        for chunk in response.iter_bytes(chunk_size=8192):
                            if self.event_abort.is_set():
                                return None

                            f.write(chunk)
                            pbar.update(len(chunk))

            except httpx.HTTPError as e:
                self.fn_logger.warning("Download attempt {}/5 failed for {}: {}", attempt, description, str(e))
                if filepath.exists():
                    filepath.unlink()

                if attempt < retries - 1:
                    sleep_time = 2**attempt
                    self.fn_logger.info("Retrying in {} seconds...", sleep_time)
                    sleep(sleep_time)

            except Exception:
                self.fn_logger.exception("Unexpected error during download of {}", description)
                if filepath.exists():
                    filepath.unlink()
                return None
            else:
                self.fn_logger.debug("Successfully downloaded: {}", description)
                return filepath

        self.fn_logger.error("Failed to download {} after 5 attempts", description)
        return None

    def _post_process(self, temp_file: Path, track: Track, stream_info: StreamInfo) -> Path | None:
        """Post-process downloaded file."""
        try:
            # Decrypt if needed
            if stream_info.is_encrypted:
                self.fn_logger.debug("Decrypting file: {}", temp_file.name)

                if not stream_info.encryption_key:
                    self.fn_logger.error("No encryption key available for decryption")
                    return None

                key, nonce = decrypt_security_token(stream_info.encryption_key)
                decrypted_file = temp_file.with_suffix(".decrypted")

                decrypt_file(temp_file, decrypted_file, key, nonce)
                temp_file = decrypted_file

            # Extract FLAC if needed (for MQA streams)
            if stream_info.file_extension == ".flac" and hasattr(stream_info, "is_mqa") and stream_info.is_mqa:
                self.fn_logger.debug("Stream is MQA, handling FLAC extraction")
                temp_file = self._extract_flac(temp_file)

            # Add metadata
            track_metadata = TrackMetaData.from_track(track)
            writer = MetadataWriter(temp_file)
            writer.write_metadata(track_metadata)

        except Exception:
            self.fn_logger.exception("Post-processing failed for file: {}", track.name)
            return None
        else:
            return temp_file

    def _extract_flac(self, temp_file: Path) -> Path:
        """Extract FLAC audio from a MQA stream."""
        self.fn_logger.debug("Extracting FLAC from MQA stream")
        output_file = temp_file.with_suffix(AudioExtensions.FLAC)
        ffmpeg = (
            FFmpeg()
            .input(url=str(temp_file))
            .output(
                url=output_file,
                map=0,
                movflags="use_metadata_tags",
                acodec="copy",
                map_metadata="0:g",
                loglevel="quiet",
            )
        )
        ffmpeg.execute()

        # Cleanup original temp file
        if temp_file.exists():
            temp_file.unlink()

        return output_file

    def _apply_download_delay(self) -> None:
        """Apply random delay between downloads."""
        if self.event_abort.is_set():
            return

        delay = uniform(*self.download_delay_range)  # noqa: S311
        self.fn_logger.debug("Applying download delay: {:.1f} seconds", delay)

        for _ in range(int(delay * 10)):
            if self.event_abort.is_set():
                break
            sleep(0.1)

    def pause(self) -> None:
        """Pause downloads."""
        self.event_pause.clear()
        self.fn_logger.info("Downloads paused.")

    def resume(self) -> None:
        """Resume downloads."""
        self.event_pause.set()
        self.fn_logger.info("Downloads resumed.")

    def stop(self) -> None:
        """Stop downloads."""
        self.event_abort.set()
        self.event_pause.set()  # In case it's paused
        self.fn_logger.info("Downloads stopping...")

    def _validate_track(self, track: Track) -> bool:
        """Validate track before download."""
        return track.available and track.duration > 0

    def _check_if_exists(self, safe_name: str, file_extension: str) -> tuple[Path, bool]:
        """Check existing files."""
        filepath = self.output_dir / f"{safe_name}{file_extension}"
        should_skip = filepath.exists() and self.skip_existing
        return filepath, should_skip

    def _finalize_download(self, temp_file: Path, final_path: Path) -> bool:
        """Finalize the download by moving the temp file to the final location."""
        try:
            temp_file.rename(final_path)
            self.fn_logger.info("Download completed: {}", final_path.name)
        except Exception:
            self.fn_logger.exception("Failed to finalize download for {}: {}", final_path.name)
            if temp_file.exists():
                temp_file.unlink()
            return False
        else:
            return True

    def download_playlist(self, tracks: list[Track]) -> dict[str, bool]:
        """Download multiple tracks from a playlist."""
        self.fn_logger.info("Starting download of {} tracks", len(tracks))

        results = {track.full_name: self.download_track(track) for track in tqdm(tracks, desc="Downloading playlist")}
        successful = sum(results.values())
        self.fn_logger.info("Downloaded {}/{} tracks successfully", successful, len(tracks))

        return results
