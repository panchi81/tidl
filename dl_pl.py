from asyncio import run
from contextlib import suppress
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
    successful = sum(1 for v in results.values() if v is True)
    total = len(results)

    logger.info("📊 Download Summary:")
    logger.info("  Total tracks: {}", total)
    logger.info("  Successful: {}", successful)
    logger.info("  Failed: {}", total - successful)

    if successful > 0:
        logger.success("✅ Downloads completed! Check the downloads folder.")
    else:
        logger.warning("⚠️ No tracks were downloaded successfully.")

def probe_entry_type(client: TidlClient, entry_id: str) -> str | None:
    """Determine if the entry ID is a track or playlist."""
    session = client.session
    http_response_ok = 200

    with suppress(Exception):
        resp = session.request.request("GET", f"/tracks/{entry_id}")
        if getattr(resp, "status_code", None) == http_response_ok or getattr(resp, "ok", False):
            return "track"

    with suppress(Exception):
        resp = session.request.request("GET", f"/playlists/{entry_id}")
        if getattr(resp, "status_code", None) == http_response_ok or getattr(resp, "ok", False):
            return "playlist"

    return None

def main() -> None:
    """Run the downloader."""
    entry_id = getenv("SINGLE_TRACK_PLAYLIST_ID")
    if not entry_id:
        logger.error("No playlist ID provided. Exiting.")
        return

    client = authenticate_client()
    if not (entry_type := probe_entry_type(client, entry_id)):
        logger.error("❌ Could not determine entry type for ID: {}", entry_id)
        sys_exit(1)

    track_service = TrackService(client.session)
    downloader = Download(track_service, client)

    if entry_type == "track":
        logger.info("📥 Deteced a track id - processing trach {}", entry_id)

    logger.info("📥 Processing...")
    results = run(downloader.orchestrate_download(entry_id))
    display_results(results)


if __name__ == "__main__":
    main()
