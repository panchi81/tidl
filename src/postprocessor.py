"""Post-processing of downloaded audio files — decryption, codec probing, FLAC extraction."""

from __future__ import annotations

from json import loads as json_loads
from pathlib import Path
from subprocess import run as subprocess_run

from ffmpeg import FFmpeg
from loguru import logger
from tidalapi.media import AudioExtensions

from src.decryption import decrypt_file, decrypt_security_token
from src.stream_info import StreamInfo


class PostProcessor:
    """Handle post-download processing: decryption, codec detection, format extraction."""

    def process(self, downloaded_file: Path, stream_info: StreamInfo) -> Path | None:
        """Post-process a downloaded file.

        Handles decryption (for non-DASH streams), codec probing, and
        FLAC extraction from MP4 containers when needed.

        Returns:
            Path to the processed file, or None on failure.

        """
        try:
            temp_file = downloaded_file

            # Decrypt if needed (DASH files are decrypted during download)
            if stream_info.is_encrypted and not stream_info.is_dash_stream:
                if not stream_info.encryption_key:
                    logger.error("Missing encryption key for {}", temp_file.name)
                    return None

                key, nonce = decrypt_security_token(stream_info.encryption_key)
                decrypted_file = temp_file.with_suffix(".decrypted")
                decrypt_file(temp_file, decrypted_file, key, nonce)
                temp_file = decrypted_file

            # Probe codec and container
            codec, container = self._probe_codec_and_container(temp_file)

            if stream_info.file_extension_atm != stream_info.predicted_file_extension:
                logger.warning(
                    "Manifest extension ({}) does not match predicted extension ({}).",
                    stream_info.file_extension_atm,
                    stream_info.predicted_file_extension,
                )

            if codec not in ("aac", "flac", "alac"):
                logger.warning("Unexpected codec detected: {} in container {}. File: {}", codec, container, temp_file)

            # Extract FLAC from MP4 container if needed
            if stream_info.needs_flac_extraction and codec == "flac" and "mp4" in container:
                logger.warning("FLAC audio in MP4 container detected. Extracting to separate FLAC file.")
                extracted_file = self._extract_flac(temp_file)
                if extracted_file.exists():
                    return extracted_file
                logger.error("FLAC extraction failed for {}", temp_file.name)
                return None

            if temp_file.exists():
                return temp_file

            logger.error("File missing {}", temp_file)
            return None

        except Exception:
            logger.exception("Post-processing failed for {}", downloaded_file.name)
            return None

    def decrypt_dash_segments(self, segment_files: list[Path], encryption_key: str) -> list[Path]:
        """Decrypt DASH segments in-place, returning list of decrypted file paths."""
        key, nonce = decrypt_security_token(encryption_key)
        decrypted_files = []
        for seg_file in segment_files:
            decrypted_file = seg_file.with_suffix(".decrypted")
            decrypt_file(seg_file, decrypted_file, key, nonce)
            decrypted_files.append(decrypted_file)
        return decrypted_files

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
            logger.exception("ffprobe failed for {}", file_path.name)
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
            logger.error("FFmpeg failed to create FLAC file: {}", output_flac_file.name)
        else:
            logger.debug("Extracted FLAC file: {}", output_flac_file.name)

        if mp4_file.exists():
            mp4_file.unlink()

        return output_flac_file
