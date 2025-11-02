from asyncio import gather
from collections.abc import Callable, Generator
from contextlib import contextmanager
from json import loads as json_loads
from pathlib import Path
from shutil import move
from subprocess import run as subprocess_run
from tempfile import TemporaryDirectory

from aiofiles import open as aio_open
from ffmpeg import FFmpeg
from httpx import AsyncClient, Client, HTTPError, Limits
from loguru import logger
from tidalapi.media import AudioExtensions, Track

from src.client import TidlClient
from src.decryption import decrypt_file, decrypt_security_token
from src.exceptions import StreamInfoError
from src.services import PlaylistService, TrackService
from src.stream_info import StreamInfo
from src.track_metadata import MetadataWriter, TrackMetaData


class Download:
    """Manage Downloads."""

    def __init__(
        self,
        track_service: TrackService,
        client: TidlClient,
        download_dir: Path = Path("./downloads"),
        fn_logger: Callable = logger,
        *,
        skip_existing: bool = False,
    ) -> None:
        """Initialize Download manager.

        Args:
            track_service (TrackService): Service to handle track operations.
            client (TidlClient): Authenticated TIDL client.
            download_dir (Path): Directory to save downloaded tracks.
            fn_logger (Callable): Logger function or object.
            skip_existing (bool): Whether to skip existing downloads.

        """
        self.track_service = track_service
        self.tdl_client = client
        self.download_dir = download_dir
        self.fn_logger = fn_logger
        self.skip_existing = skip_existing
        self.httpx_client = Client(timeout=30.0, limits=Limits(max_connections=10, max_keepalive_connections=5))

    async def orchestrate_download(self, playlist_id: str) -> dict[str, bool]:
        """Manage download process."""
        playlist_name, tracks = self.resolve_tracks_from_playlist(playlist_id)
        self.fn_logger.info("Preparing to download {} tracks", len(tracks))
        self.download_dir = self.download_dir / playlist_name
        tasks = [self.process_track(track) for track in tracks]
        result_list = await gather(*tasks, return_exceptions=True)
        results = {track.full_name: result for track, result in zip(tracks, result_list, strict=False)}
        progress = sum(v is True for v in results.values())
        self.fn_logger.info("Downloaded {}/{} tracks successfully", progress, len(tracks))
        return results

    def resolve_tracks_from_playlist(self, playlist_id: str) -> tuple[str, list[Track]]:
        """Fetch tracks from a playlist by ID."""
        playlist_service = PlaylistService(self.tdl_client.session)
        playlist = playlist_service.get_playlist(playlist_id)
        tracks = playlist_service.get_playlist_tracks(playlist)
        self.fn_logger.info("Found playlist: {} with {} tracks", playlist.name, playlist.get_tracks_count())
        return playlist.name, tracks

    async def process_track(self, track: Track) -> bool:
        """Process track data."""
        # Validation
        if not self._validate_track(track):
            return False

        try:
            stream_info = self.track_service.get_stream_info(track, self.tdl_client)
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
                return await self._process_download(stream_info, track, workspace, final_path)
        except Exception:
            self.fn_logger.exception("Failed to download track: {}", track.full_name)
            return False

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
                self.fn_logger.debug("Successfully downloaded: {}", description)
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
