from asyncio import run as asyncio_run
from datetime import UTC, datetime
from os import getenv
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError
from subprocess import run as subprocess_run
from sys import exit as sys_exit
from tempfile import mkdtemp
from typing import Annotated

import smello
import typer
from dotenv import load_dotenv
from httpx import ConnectError, TimeoutException, get
from loguru import logger
from src.client import TidlClient
from src.dl import Download
from src.services import TrackService
from src.setup_logging import setup_logging

app = typer.Typer()
load_dotenv()
setup_logging()


def authenticate_client() -> TidlClient:
    """Authenticate and return client."""
    logger.info("🔐 Initializing client...")
    client = TidlClient()

    logger.info("🔑 Authenticating with TIDAL...")
    if not client.authenticate_pkce():
        logger.error("❌ Authentication failed. Check your credentials.")
        sys_exit(1)

    logger.success("✅ Authentication successful!")
    return client


def display_results(results: dict[str, bool]) -> None:
    """Display download results."""
    successful = sum(v is True for v in results.values())
    total = len(results)

    logger.info("📊 Download Summary:")
    logger.info("  Total tracks: {}", total)
    logger.info("  Successful: {}", successful)
    logger.info("  Failed: {}", total - successful)

    if successful > 0:
        logger.success("✅ Downloads completed! Check the downloads folder.")
    else:
        logger.warning("⚠️ No tracks were downloaded successfully.")


@app.command()
def main(*, test_run: Annotated[bool, typer.Option("--test-run", help="Run Api Tests")] = False) -> None:
    """Run the downloader."""
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

        download_dir = Path("./downloads")
        skip_existing = True
        skip_db = False
    if not playlist_id:
        logger.error("No playlist ID provided. Exiting.")
        return

    client = authenticate_client()
    track_service = TrackService(client.session)
    downloader = Download(
        track_service=track_service,
        client=client,
        download_dir=download_dir,
        skip_existing=skip_existing,
        skip_db=skip_db,
        batch_size=4,
        concurrent_downloads=2,
        batch_delay=6,
        api_delay=0.25,
    )

    logger.info("📥 Processing...")
    results = asyncio_run(downloader.orchestrate_download(playlist_id))
    display_results(results)

    # Cleanup temp dir on success, keep on failure
    if test_run:
        smello.flush(timeout=5.0)
        failed = sum(1 for v in results.values() if not v)
        if failed == 0:
            rmtree(download_dir)
            logger.info("Test run complete, temp dir cleaned up")
        else:
            logger.warning("Test run had failures, keeping temp dir: {}", download_dir)


def _get_session_name() -> str:
    """Get a session name for Smello."""
    try:
        branch = subprocess_run(
            ["/usr/bin/git", "branch", "--show-current"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (CalledProcessError, FileNotFoundError):
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
