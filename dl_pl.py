"""tidl — TIDAL playlist downloader."""

from datetime import UTC, datetime
from os import getenv
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError, run
from sys import exit as sys_exit
from tempfile import mkdtemp
from typing import Annotated

import smello
import typer
from dotenv import load_dotenv
from httpx import ConnectError, TimeoutException, get
from loguru import logger
from src.auth import Authenticator
from src.client import TidlClient
from src.orchestrator import DownloadConfig, DownloadOrchestrator, TrackResult
from src.setup_logging import setup_logging

app = typer.Typer()
load_dotenv()
setup_logging()


@app.command()
def main(test_run: Annotated[bool, typer.Option("--test-run", help="Run Api Tests")] = False) -> None:
    """Download a TIDAL playlist."""
    download_dir: Path

    if test_run:
        logger.info("Running in test mode. TEST_PL, temp dir, skip DB")
        playlist_id = getenv("TEST_PL")

        server_url = getenv("SMELLO_URL", "http://localhost:5110")
        _ensure_smello_server(server_url)

        download_dir = Path(mkdtemp(prefix="test-pl-"))
        skip_existing = False
        skip_db = True
        logger.info("Test run: downloading to {}", download_dir)
        smello.init(
            server_url=server_url,
            app="tidl",
            session=_get_session_name(),
            capture_logs=True,
            log_level=20,  # INFO
        )
        logger.info("Smello initialized for test run")
    else:
        playlist_id = getenv("AHORATEVAS")

        download_dir = Path(getenv("DOWNLOAD_DIR", "./downloads"))
        skip_existing = True
        skip_db = False
    if not playlist_id:
        logger.error("Invalid or no playlist ID provided.")
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
        download_dir=download_dir,
        skip_existing=skip_existing,
        skip_db=skip_db,
        concurrent_downloads=int(getenv("CONCURRENT_DOWNLOADS", "2")),
        requests_per_second=int(getenv("REQUESTS_PER_SECOND", "4")),
    )

    with DownloadOrchestrator(client, config) as orchestrator:
        results = orchestrator.download_playlist(playlist_id)

    _display_results(results)

    # Cleanup temp dir on success, keep on failure
    if test_run:
        smello.flush(timeout=5.0)
        failed = sum(1 for r in results if not r.success)
        if failed == 0:
            rmtree(download_dir)
            logger.info("Test run complete, temp dir cleaned up")
        else:
            logger.warning("Test run had failures, keeping temp dir: {}", download_dir)


def _display_results(results: list[TrackResult]) -> None:
    """Display download summary."""
    downloaded = sum(1 for r in results if r.success and not r.skipped)
    skipped = sum(1 for r in results if r.skipped)
    failed = sum(1 for r in results if not r.success)

    logger.info(
        "Download Summary: {} total, {} successful, {} skipped, {} failed", len(results), downloaded, skipped, failed
    )

    if failed > 0:
        for r in results:
            if not r.success:
                logger.warning("  FAILED: {} — {}", r.track_name, r.reason)


def _get_session_name() -> str:
    """Get a session name for Smello."""
    try:
        branch = run(
            ["/usr/bin/git", "branch", "--show-current"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except CalledProcessError, FileNotFoundError:
        branch = "unknown"
    timestamp = datetime.now(tz=UTC).strftime("%H%M")
    return f"{branch}-{timestamp}"


def _ensure_smello_server(server_url: str) -> None:
    """Check smello server is reachable."""
    response_status_ok = 200
    try:
        response = get(f"{server_url}/api/events", timeout=2.0)
        if response.status_code != response_status_ok:
            logger.error("Smello server at {} returned status {}", server_url, response.status_code)
            sys_exit(1)
    except ConnectError:
        logger.error("Smello server not reachable at {}. Start it with: smello-server", server_url)
        sys_exit(1)
    except TimeoutException:
        logger.error("Smello server at {} timed out", server_url)
        sys_exit(1)


if __name__ == "__main__":
    app()
