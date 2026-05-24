"""File management for downloads — workspace, paths, and finalization."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Generator


class FileManager:
    """Manage download workspace, path resolution, and file finalization."""

    def __init__(self, download_dir: Path, *, skip_existing: bool = False) -> None:
        self.download_dir = download_dir
        self.skip_existing = skip_existing

    def check_if_exists(self, safe_name: str, file_extension: str) -> tuple[Path, bool]:
        """Check if a file already exists on disk.

        Returns:
            Tuple of (final_path, should_skip).

        """
        filepath = self.download_dir / f"{safe_name}{file_extension}"
        should_skip = filepath.exists() and self.skip_existing
        return filepath, should_skip

    def finalize(self, processed_file: Path, final_path: Path) -> Path:
        """Move processed file to its final location.

        Returns:
            The actual target path (suffix may differ from final_path if processed_file has different extension).

        Raises:
            FileNotFoundError: If processed_file does not exist.

        """
        if not processed_file.exists():
            msg = f"File does not exist and cannot be finalized: {processed_file}"
            raise FileNotFoundError(msg)

        target_path = final_path.with_suffix(processed_file.suffix)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        move(str(processed_file), str(target_path))
        logger.info("Moved {} to {}", processed_file.name, target_path.name)
        return target_path

    @staticmethod
    @contextmanager
    def workspace(track_name: str) -> Generator[Path]:
        """Context manager for a temporary download workspace with cleanup."""
        safe_name = "".join(c for c in track_name if c.isalnum() or c in ("-", "_"))[:50]

        with TemporaryDirectory(prefix=f"tidl_{safe_name}_") as temp_dir:
            workspace_path = Path(temp_dir)
            yield workspace_path
            logger.debug("Cleaning up download workspace")
