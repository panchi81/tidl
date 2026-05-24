"""Stream download strategies — standard single-file and DASH multi-segment."""

from __future__ import annotations

from pathlib import Path

from httpx import Client, HTTPError
from loguru import logger
from streamable import stream
from tidalapi.media import AudioExtensions, Track

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
        except Exception:
            logger.exception("Unexpected error during download of {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        else:
            return filepath


class DashStreamStrategy:
    """Download DASH multi-segment streams with concurrent segment fetching."""

    def __init__(self, http_client: Client, postprocessor: PostProcessor, *, segment_concurrency: int = 4) -> None:
        self.http_client = http_client
        self.postprocessor = postprocessor
        self.segment_concurrency = segment_concurrency

    def download(self, stream_info: StreamInfo, track: Track, workspace: Path) -> Path | None:
        """Download all DASH segments, decrypt if needed, and merge."""
        segments_dir = workspace / "segments"
        segments_dir.mkdir(exist_ok=True)

        # Build segment file paths
        segment_targets = self._plan_segments(stream_info, segments_dir)

        # Download segments concurrently via streamable
        downloaded = list(
            stream(segment_targets)
            .map(lambda t: self._download_segment(t[0], t[1], track.name), concurrency=self.segment_concurrency)
        )

        # Filter out failed segments
        segment_files = [p for p in downloaded if p is not None]
        if not segment_files:
            logger.error("All DASH segments failed for {}", track.name)
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

    def _download_segment(self, url: str, filepath: Path, description: str) -> Path | None:
        """Download a single segment."""
        try:
            response = self.http_client.get(url)
            response.raise_for_status()
            with filepath.open("wb") as f:
                f.write(response.content)
        except HTTPError:
            logger.exception("Failed to download segment for {}", description)
            if filepath.exists():
                filepath.unlink()
            return None
        except Exception:
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
            except (ValueError, IndexError):
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
