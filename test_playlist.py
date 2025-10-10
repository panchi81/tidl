from pathlib import Path

from dotenv import dotenv_values
from loguru import logger
from src.client import TidlClient
from src.setup_logging import setup_logging

setup_logging()

env_path = Path("../.env")
env=dotenv_values(dotenv_path=env_path)
TDL_CLIENT_ID = env.get("tdl_client_id")
TDL_CLIENT_SECRET = env.get("tdl_client_secret")
PLAYLIST_ID = env.get("PLAYLIST_ID")


def playlist_test() -> None:
    """Test fetching playlist tracks."""
    client = TidlClient()

    logger.info("Authenticating...")
    if not client.authenticate():
        logger.error("Authentication failed. Cannot proceed with playlist test.")
        return

    playlist_id = PLAYLIST_ID

    try:
        logger.info("Fetching tracks from playlist...")
        tracks = client.get_playlist_tracks(playlist_id)
    except Exception as e:  # noqa: BLE001
        logger.error("An error occurred while fetching playlist tracks")
        logger.exception(e)
    else:
        if tracks is None:
            logger.error("No tracks found or unable to fetch tracks.")
            return

        logger.info("Found {} tracks in playlist.", len(tracks))

        for i, track in enumerate(tracks, start=1):
            track_info = f"{track.artist.name} - {track.name}"
            logger.info("{}. {}", i, track_info)


def test_detailed_metadata() -> None:
    """Test fetching detailed track metadata."""
    client = TidlClient()

    # Authenticate
    logger.info("Authenticating...")
    if not client.authenticate():
        logger.error("Authentication failed. Cannot proceed.")
        return

    playlist_id = PLAYLIST_ID

    try:
        logger.info("Fetching detailed metadata...")
        detailed_tracks = client.get_playlist_tracks_detailed(playlist_id)

        logger.info("=== DETAILED TRACK METADATA ===")

        for i, track in enumerate(detailed_tracks, start=1):
            logger.info("{}. {}", i, track.full_title)
            logger.info("   Album: {} ({})", track.album, track.release_year or "Unknown year")
            logger.info("   Duration: {}", track.duration_formatted)
            logger.info("   Quality: {}", track.quality.value if track.quality else "Unknown")
            logger.info("   Genre: {}", track.genre or "Unknown")
            logger.info("   Hi-res: {}", "Yes" if track.is_hi_res else "No")
            logger.info("   ISRC: {}", track.isrc or "Unknown")
            logger.info("   URL: {}", track.url)
            logger.info("")  # Empty line for readability

    except Exception as e:  # noqa: BLE001
        logger.exception("Error fetching detailed metadata: {}", e)


if __name__ == "__main__":
    # playlist_test()
    test_detailed_metadata()
