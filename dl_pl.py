"""tidl — TIDAL playlist downloader."""

from os import getenv
from pathlib import Path
from sys import exit as sys_exit

from dotenv import load_dotenv
from loguru import logger
from src.auth import Authenticator
from src.client import TidlClient
from src.orchestrator import DownloadConfig, DownloadOrchestrator, TrackResult
from src.setup_logging import setup_logging

load_dotenv()
setup_logging()


def main() -> None:
    """Download a TIDAL playlist."""
    playlist_id = getenv("PLAYLIST_ID")
    if not playlist_id:
        logger.error("No playlist ID provided. Set PLAYLIST_ID environment variable.")
        sys_exit(1)

    # Authenticate
    client = TidlClient()
    auth = Authenticator(client.session)

    logger.info("Authenticating with TIDAL...")
    if not auth.authenticate_pkce():
        logger.error("Authentication failed.")
        sys_exit(1)
    logger.info("Authentication successful.")

    # Configure and run
    config = DownloadConfig(
        download_dir=Path(getenv("DOWNLOAD_DIR", "./downloads")),
        skip_existing=True,
        concurrent_downloads=int(getenv("CONCURRENT_DOWNLOADS", "2")),
        requests_per_second=int(getenv("REQUESTS_PER_SECOND", "4")),
    )

    with DownloadOrchestrator(client, config) as orchestrator:
        results = orchestrator.download_playlist(playlist_id)

    _display_results(results)


def _display_results(results: list[TrackResult]) -> None:
    """Display download summary."""
    successful = sum(1 for r in results if r.success)
    skipped = sum(1 for r in results if r.skipped)
    failed = sum(1 for r in results if not r.success)

    logger.info(
        "Download Summary: {} total, {} successful, {} skipped, {} failed",
        len(results), successful, skipped, failed,
    )

    if failed > 0:
        for r in results:
            if not r.success:
                logger.warning("  FAILED: {} — {}", r.track_name, r.reason)


if __name__ == "__main__":
    main()
