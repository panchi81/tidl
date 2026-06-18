"""Stream download strategies — standard single-file and DASH multi-segment."""

from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING

from httpx import Client, HTTPError, TimeoutException
from loguru import logger
from streamable import stream
from tidalapi.media import AudioExtensions, Track

if TYPE_CHECKING:
    from pathlib import Path

    from src.postprocessor import PostProcessor
    from src.stream_info import StreamInfo


class StandardStreamStrategy:
    """Download a single-URL stream synchronously."""

    def __init__(self, http_client: Client) -> None:
        self.http_client = http_client

    def download(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download the first URL from stream_info to workspace."""
        temp_file = workspace / f"download{stream_info.file_extension_atm}"
        url = stream_info.urls[0]
        return self._download(url, temp_file, track.name)

    def _download(self, url: str, filepath: Path, description: str) -> Path | None:
        """Download a single file synchronously."""
        try:
            response = self.http_client.get(url)
            response.raise_for_status()
            with filepath.open("wb") as f:
                f.write(response.content)
        except HTTPError:
            logger.exception("Failed to download {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        except OSError:
            logger.exception("Unexpected error during download of {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        else:
            return filepath


class DashStreamStrategy:
    """Download DASH multi-segment streams with concurrent segment fetching."""

    def __init__(
        self,
        http_client: Client,
        postprocessor: PostProcessor,
        *,
        segment_concurrency: int = 4,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
    ) -> None:
        self.http_client = http_client
        self.postprocessor = postprocessor
        self.segment_concurrency = segment_concurrency
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay

    def download(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download all DASH segments, decrypt if needed, and merge."""
        segments_dir = workspace / "segments"
        segments_dir.mkdir(exist_ok=True)

        # Build segment file paths
        segment_targets = self._plan_segments(stream_info, segments_dir)
        expected_count = len(segment_targets)

        # Download segments concurrently via streamable
        downloaded = list(
            stream(segment_targets).map(
                lambda t: self._download_segment_with_retry(t[0], t[1], track.name),
                concurrency=self.segment_concurrency,
            )
        )

        # Validate: ALL segments must succeed
        segment_files = [p for p in downloaded if p is not None]
        failed_count = expected_count - len(segment_files)

        if failed_count > 0:
            logger.error(
                "DASH download failed for {}: {}/{} segments missing", track.name, failed_count, expected_count
            )
            return None

        # Decrypt if needed
        if stream_info.is_encrypted and stream_info.encryption_key:
            segment_files = self.postprocessor.decrypt_dash_segments(segment_files, stream_info.encryption_key)

        # Merge segments
        merged_file = workspace / f"merged{stream_info.file_extension_atm}"
        self._merge_segments(segment_files, merged_file)

        if merged_file.exists():
            return merged_file
        return None

    def _plan_segments(self, stream_info: StreamInfo, segments_dir: Path) -> list[tuple[str, Path]]:
        """Build list of (url, target_path) for each segment."""
        targets = []
        for url in stream_info.urls:
            url_filename = url.split("/")[-1].split("?")[0]
            filename_stem = url_filename.split("_")[-1].split(".")[0]
            segment_id = int(filename_stem) if filename_stem.isdecimal() else 0

            file_extension = (
                AudioExtensions.FLAC if stream_info.needs_flac_extraction else stream_info.file_extension_atm
            )
            segment_file = segments_dir / f"segment_{segment_id:03d}{file_extension}"
            targets.append((url, segment_file))
        return targets

    def _download_segment_with_retry(self, url: str, filepath: Path, description: str) -> Path | None:
        """Download a segment with exponential backoff retry."""
        for attempt in range(self.max_retries):
            result = self._download_segment(url, filepath, description)
            if result is not None:
                return result

            if attempt < self.max_retries - 1:
                delay = self.retry_base_delay * (2**attempt)
                logger.warning(
                    "Retrying segment for {} (attempt {}/{}, backoff {:.1f}s)",
                    description,
                    attempt + 2,
                    self.max_retries,
                    delay,
                )
                sleep(delay)

        logger.error("Segment download exhausted all retries for {}", description)
        return None

    def _download_segment(self, url: str, filepath: Path, description: str) -> Path | None:
        """Download a single segment."""
        try:
            response = self.http_client.get(url)
            response.raise_for_status()
            with filepath.open("wb") as f:
                f.write(response.content)
        except HTTPError, TimeoutException:
            logger.exception("Failed to download segment for {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        except OSError:
            logger.exception("Unexpected error downloading segment for {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        else:
            return filepath

    def _merge_segments(self, segment_files: list[Path], output_file: Path) -> None:
        """Merge DASH segments into a single file via binary concatenation."""

        def get_segment_id(path: Path) -> int:
            try:
                return int(path.stem.split("_")[-1])
            except ValueError, IndexError:
                return 0

        sorted_segments = sorted(segment_files, key=get_segment_id)
        chunk_size = 4 * 1024 * 1024  # 4 MB

        with output_file.open("wb") as f_target:
            for i, segment_file in enumerate(sorted_segments, start=1):
                if not segment_file.exists():
                    logger.error("Segment file missing: {}", segment_file.name)
                    continue

                logger.debug("Merging segment {}/{}: {}", i, len(sorted_segments), segment_file.name)
                with segment_file.open("rb") as f_segment:
                    while chunk := f_segment.read(chunk_size):
                        f_target.write(chunk)

        logger.debug("Successfully merged {} segments into {}", len(sorted_segments), output_file)
