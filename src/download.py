import shutil
import signal
import subprocess
from atexit import register
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from random import uniform
from tempfile import TemporaryDirectory
from threading import Event
from time import sleep
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from src.client import TidlClient


class Download:
    """Main class for managing downloads."""

    def __init__(  # noqa: PLR0913
        self,
        track_service: TrackService,
        client: "TidlClient",
        output_dir: Path | str = "./downloads",
        fn_logger: Callable = logger,
        download_delay_range: tuple[int, int] = (3, 6),
        *,
        skip_existing: bool = True,
    ) -> None:
        """Initialize the Downloader object and its attributes.

        Args:
            track_service: Service for track operations
            client: TIDAL client for authentication and API access
            output_dir (Path | str, optional): Base path for downloads. Defaults to "./downloads".
            fn_logger (Callable, optional): Logger function for logging events. Defaults to logger.
            download_delay_range: Range for random delays between downloads
            skip_existing (bool, optional): Whether to skip downloading files that already exist. Defaults to True.

        """
        self.track_service = track_service
        self.tidal_client = client
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
            stream_info = self.track_service.get_stream_info(track, self.tidal_client)
        except StreamInfoError:
            self.fn_logger.exception("Failed to get stream info for track: {}", track.full_name)
            return False

        # Check if exists
        safe_name = self.track_service.get_track_safe_name(track)
        final_path, should_skip = self._check_if_exists(safe_name, stream_info.file_extension_atm)

        if should_skip:
            self.fn_logger.info("Skipping existing file: {}", final_path.name)
            return True

        # Download
        try:
            with self.download_workspace(track.name) as workspace:
                return self._process_download(stream_info, track, workspace, final_path)
        except Exception:
            self.fn_logger.exception("Failed to download track: {}", track.full_name)
            return False

    def _process_download(self, stream_info: StreamInfo, track: Track, workspace: Path, final_path: Path) -> bool:
        if self.event_abort.is_set():
            return False

        downloaded_file = self._download_to_workspace(stream_info, track, workspace)
        if not downloaded_file or not downloaded_file.exists():
            return False

        processed_file = self._post_process(downloaded_file, track, stream_info)
        if not processed_file or not processed_file.exists():
            return False

        success = self._finalize_download(processed_file, final_path, track)
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
        temp_file = workspace / f"download{stream_info.file_extension_atm}"
        url = stream_info.urls[0]

        return self._download_file(url, temp_file, track.name)

    def _download_dash_segments(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:  # noqa: C901, PLR0911, PLR0912, PLR0915
        """Download DASH segments."""
        segments_dir = workspace / "segments"
        segments_dir.mkdir(exist_ok=True)

        self.fn_logger.info("Downloading {} DASH segments for {}", len(stream_info.urls), track.name)

        segment_files = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            for url in stream_info.urls:
                if self.event_abort.is_set():
                    break

                # Extract segment ID from URL filename
                url_filename = url.split("/")[-1].split("?")[0]  # Get filename, remove query params
                # Get the part after last underscore, before extension
                filename_stem = url_filename.split("_")[-1].split(".")[0]
                segment_id = int(filename_stem) if filename_stem.isdecimal() else 0

                # Debug: Log URL structure for first few segments
                test_segments: int = 3
                if len(futures) < test_segments:  # Only log first 3 to avoid spam
                    self.fn_logger.debug("URL: {}", url)
                    self.fn_logger.debug("Filename: {}, Stem: {}, ID: {}", url_filename, filename_stem, segment_id)

                segment_file = segments_dir / f"segment_{segment_id:03d}{stream_info.file_extension_atm}"
                future = executor.submit(
                    self._download_file, url, segment_file, f"Segment {segment_id}/{len(stream_info.urls)}"
                )
                futures.append((future, segment_file, segment_id))

            for future, segment_file, _segment_id in futures:
                if self.event_abort.is_set():
                    future.cancel()
                    return None

                try:
                    if result_file := future.result(timeout=60):
                        segment_files.append(result_file)
                        # Debug: Check segment size
                        if result_file.exists():
                            size = result_file.stat().st_size
                            self.fn_logger.debug("Downloaded segment {}: {} bytes", result_file.name, size)
                    else:
                        self.fn_logger.error("Failed to download segment: {}", segment_file.name)
                        return None
                except Exception:
                    self.fn_logger.exception("Segment download error")
                    return None

        if len(segment_files) != len(stream_info.urls):
            self.fn_logger.error("Not all segments were downloaded successfully")
            return None

        # Decrypt segments if needed before merging
        if stream_info.is_encrypted:
            self.fn_logger.debug("Decrypting {} DASH segments", len(segment_files))

            if not stream_info.encryption_key:
                self.fn_logger.error("No encryption key available for segment decryption")
                return None

            key, nonce = decrypt_security_token(stream_info.encryption_key)

            decrypted_segments = []
            for segment_file in segment_files:
                decrypted_file = segment_file.with_suffix(".decrypted" + stream_info.file_extension_atm)
                try:
                    decrypt_file(segment_file, decrypted_file, key, nonce)
                    decrypted_segments.append(decrypted_file)
                    self.fn_logger.debug("Decrypted segment: {}", segment_file.name)
                except Exception:
                    self.fn_logger.exception("Failed to decrypt segment: {}", segment_file.name)
                    return None

            segment_files = decrypted_segments

        merged_file = workspace / f"merged{stream_info.file_extension_atm}"
        self._merge_segments(segment_files, merged_file)

        # Debug: Check merged file size
        kilobyte = 1024
        if merged_file.exists():
            merged_size = merged_file.stat().st_size
            self.fn_logger.info("Merged file created: {} bytes", merged_size)
            if merged_size < kilobyte:
                self.fn_logger.error("Merged file is suspiciously small: {} bytes", merged_size)
        else:
            self.fn_logger.error("Merged file was not created!")
            return None

        return merged_file

    def _merge_segments(self, segment_files: list[Path], output_file: Path) -> None:
        """Merge DASH segments into a single audio file using binary concatenation.

        Uses the same approach as tidal-dl-ng: simple binary file concatenation
        instead of ffmpeg, which works better with DASH segments.
        """
        try:
            self.fn_logger.debug("Merging {} segments using binary concatenation", len(segment_files))

            # Sort segments by their numeric ID to ensure correct order
            def get_segment_id(path: Path) -> int:
                # Extract number from filename like "segment_001.m4a" -> 1
                try:
                    return int(path.stem.split("_")[-1])
                except (ValueError, IndexError):
                    return 0

            sorted_segments = sorted(segment_files, key=get_segment_id)

            # Binary concatenation - same as tidal-dl-ng approach
            chunk_size = 4 * 1024 * 1024  # 4MB chunks for better performance

            with output_file.open("wb") as f_target:
                for i, segment_file in enumerate(sorted_segments):
                    if not segment_file.exists():
                        self.fn_logger.error("Segment file missing: {}", segment_file)
                        continue

                    self.fn_logger.debug("Merging segment {}/{}: {}", i + 1, len(sorted_segments), segment_file.name)

                    with segment_file.open("rb") as f_segment:
                        while chunk := f_segment.read(chunk_size):
                            f_target.write(chunk)

            self.fn_logger.debug("Successfully merged {} segments into {}", len(sorted_segments), output_file)

            # Verify output file
            kilobyte = 1024
            if output_file.exists():
                output_size = output_file.stat().st_size
                self.fn_logger.debug("Binary merge output file size: {} bytes", output_size)
                if output_size < kilobyte:
                    self.fn_logger.error("Binary merge created suspiciously small file: {} bytes", output_size)

                # Post-process with ffmpeg to create proper MP4 container for metadata
                self._containerize_merged_file(output_file)
            else:
                self.fn_logger.error("Binary merge did not create output file: {}", output_file)

        except Exception:
            self.fn_logger.exception("Error during binary segment merging")
            raise

    def _containerize_merged_file(self, merged_file: Path) -> None:
        """Use ffmpeg to properly containerize the binary-merged file for metadata compatibility.

        This creates a proper MP4 container structure that mutagen can read for metadata writing.
        """
        try:
            # Check if ffmpeg is available
            if not shutil.which("ffmpeg"):
                self.fn_logger.warning("ffmpeg not found - merged file may not support metadata")
                return

            # Check if file is FLAC - skip containerization as FLAC files are already properly formatted
            # and M4A containers don't support FLAC codec
            probe_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_name",
                "-of",
                "csv=p=0",
                str(merged_file),
            ]

            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10, check=False)

            if result.returncode == 0 and "flac" in result.stdout.lower():
                self.fn_logger.debug("Skipping containerization for FLAC file - already properly formatted")
                return

            # Create temporary containerized file
            temp_containerized = merged_file.with_suffix(".containerized.m4a")

            # Use ffmpeg to copy/containerize without re-encoding
            # -c copy preserves the audio stream without re-encoding
            # -movflags faststart optimizes for streaming/metadata
            cmd = [
                "ffmpeg",
                "-y",  # Overwrite output file
                "-i",
                str(merged_file),
                "-c",
                "copy",  # Copy streams without re-encoding
                "-movflags",
                "faststart",  # Better MP4 structure
                str(temp_containerized),
            ]

            self.fn_logger.debug("Containerizing merged file with ffmpeg...")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,  # 30 second timeout
                check=False,  # Don't raise on non-zero exit
            )

            if result.returncode == 0:
                # Replace original with containerized version
                shutil.move(str(temp_containerized), str(merged_file))
                self.fn_logger.debug("Successfully containerized merged file")
            else:
                self.fn_logger.warning("ffmpeg containerization failed: {}", result.stderr)
                # Clean up temp file if it exists
                if temp_containerized.exists():
                    temp_containerized.unlink()

        except Exception:
            self.fn_logger.exception("Error during file containerization")
            # Don't raise - this is a post-processing enhancement, not critical

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
            # Decrypt if needed (skip for DASH files as they're already decrypted)
            is_dash_stream = (
                hasattr(stream_info, "urls") and isinstance(stream_info.urls, list) and len(stream_info.urls) > 1
            )

            if stream_info.is_encrypted and not is_dash_stream:
                self.fn_logger.debug("Decrypting file: {}", temp_file.name)

                if not stream_info.encryption_key:
                    self.fn_logger.error("No encryption key available for decryption")
                    return None

                key, nonce = decrypt_security_token(stream_info.encryption_key)
                decrypted_file = temp_file.with_suffix(".decrypted")

                decrypt_file(temp_file, decrypted_file, key, nonce)
                temp_file = decrypted_file
            elif is_dash_stream:
                self.fn_logger.debug("Skipping decryption for DASH stream (already decrypted)")

            # Extract FLAC if needed (for MQA streams)
            if stream_info.file_extension_atm == ".flac" and hasattr(stream_info, "is_mqa") and stream_info.is_mqa:
                self.fn_logger.debug("Stream is MQA, handling FLAC extraction")
                temp_file = self._extract_flac(temp_file)

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

    def _finalize_download(self, temp_file: Path, final_path: Path, track: Track) -> bool:
        """Finalize the download by moving the temp file to the final location and adding metadata."""
        try:
            # Check if we need to correct the file extension based on actual codec
            corrected_final_path = self._correct_file_extension(temp_file, final_path)

            temp_file.rename(corrected_final_path)
            self.fn_logger.info("Download completed: {}", corrected_final_path.name)

            # Add metadata to the finalized file
            self._add_metadata_to_file(corrected_final_path, track)

        except Exception:
            self.fn_logger.exception("Failed to finalize download for {}: {}", final_path.name)
            if temp_file.exists():
                temp_file.unlink()
            return False
        else:
            return True

    def _correct_file_extension(self, temp_file: Path, final_path: Path) -> Path:
        """Correct file extension based on actual audio codec."""
        try:
            # Use ffprobe to detect the actual codec
            probe_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_name",
                "-of",
                "csv=p=0",
                str(temp_file),
            ]

            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10, check=False)

            if result.returncode == 0:
                codec = result.stdout.strip().lower()
                self.fn_logger.debug("Detected codec: {}", codec)

                # If it's FLAC codec in MP4 container, change to .flac
                # This is necessary because TIDAL DASH streams put FLAC audio in MP4 containers
                # but the MetadataWriter needs them to be named .flac to handle them properly
                if "flac" in codec and final_path.suffix.lower() == ".m4a":
                    corrected_path = final_path.with_suffix(".flac")
                    self.fn_logger.info(
                        "Correcting extension for FLAC codec: {} -> {}", final_path.name, corrected_path.name
                    )
                    return corrected_path

        except Exception as e:
            self.fn_logger.debug("Could not detect codec, keeping original extension: {}", str(e))

        return final_path

    def _add_metadata_to_file(self, file_path: Path, track: Track) -> None:
        """Add metadata to the final audio file."""
        try:
            track_metadata = TrackMetaData.from_track(track)
            writer = MetadataWriter(file_path)
            writer.write_metadata(track_metadata)
            self.fn_logger.debug("Metadata added to: {}", file_path.name)

        except Exception as e:
            self.fn_logger.warning("Failed to add metadata to {}: {}", file_path.name, str(e))

    def download_playlist(self, tracks: list[Track]) -> dict[str, bool]:
        """Download multiple tracks from a playlist."""
        self.fn_logger.info("Starting download of {} tracks", len(tracks))

        results = {track.full_name: self.download_track(track) for track in tqdm(tracks, desc="Downloading playlist")}
        successful = sum(results.values())
        self.fn_logger.info("Downloaded {}/{} tracks successfully", successful, len(tracks))

        return results
