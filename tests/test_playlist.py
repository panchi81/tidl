from loguru import logger
from src.client import TidlClient
from src.setup_logging import setup_logging

setup_logging()


def playlist_test() -> None:
    """Test fetching playlist tracks."""
    client = TidlClient()

    logger.info("Authenticating...")
    if not client.authenticate():
        logger.error("Authentication failed. Cannot proceed with playlist test.")
        return

    playlist_id = "2465db05-ad4b-49d7-ae75-02c72bca7af0"

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

    # Your Salsa Moderna playlist
    playlist_id = "2465db05-ad4b-49d7-ae75-02c72bca7af0"

    try:
        logger.info("Fetching detailed metadata...")
        detailed_tracks = client.get_playlist_tracks_detailed(playlist_id)

        logger.info("=== DETAILED TRACK METADATA ===")

        for i, track in enumerate(detailed_tracks, start=1):
            logger.info("{}. {}", i, track.full_title)
            logger.info("   Album: {} ({})", track.album, track.release_year or "Unknown year")
            logger.info("   Duration: {}", track.duration_s)
            logger.info("   Quality: {}", track.quality.value if track.quality else "Unknown")
            logger.info("   Genre: {}", track.genre or "Unknown")
            logger.info("   Hi-res: {}", "Yes" if track.is_hi_res else "No")
            logger.info("   ISRC: {}", track.isrc or "Unknown")
            logger.info("   URL: {}", track.url)
            logger.info("   Stream URL: {}", track.stream_url or "Unavailable")

            if track.stream_url:
                if isinstance(track.stream_url, list):
                    logger.info("   Stream URLs: {} DASH segments", len(track.stream_url))
                    logger.info(
                        "   First segment: {}",
                        track.stream_url[0][:100] + "..." if len(track.stream_url[0]) > 100 else track.stream_url[0],  # noqa: PLR2004
                    )
                else:
                    logger.info(
                        "   Stream URL: {}",
                        track.stream_url[:100] + "..." if len(track.stream_url) > 100 else track.stream_url,  # noqa: PLR2004
                    )
            else:
                logger.info("   Stream URL: Unavailable")
            logger.info("")

    except Exception as e:  # noqa: BLE001
        logger.exception("Error fetching detailed metadata: {}", e)


if __name__ == "__main__":
    # playlist_test()
    test_detailed_metadata()
