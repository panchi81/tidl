from asyncio import run
from os import getenv
from sys import exit as sys_exit

from dotenv import load_dotenv
from loguru import logger
from src.client import TidlClient
from src.dl import Download
from src.services import TrackService
from src.setup_logging import setup_logging

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


def main() -> None:
    """Run the downloader."""
    playlist_id = getenv("PLAYLIST_ID")
    if not playlist_id:
        logger.error("No playlist ID provided. Exiting.")
        return

    client = authenticate_client()
    track_service = TrackService(client.session)
    downloader = Download(track_service, client)

    logger.info("📥 Processing...")
    results = run(downloader.orchestrate_download(playlist_id))
    display_results(results)


if __name__ == "__main__":
    main()
