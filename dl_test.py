from os import getenv
from typing import TYPE_CHECKING

from dotenv import read_dotenv
from loguru import logger
from src.client import TidlClient
from src.services import PlaylistService
from src.setup_logging import setup_logging

if TYPE_CHECKING:
    from tidalapi.media import Track
    from tidalapi.playlist import Playlist

read_dotenv()
setup_logging()

PLAYLIST_ID = getenv("PLAYLIST_ID")


def playlist_test(playlist_id: str = PLAYLIST_ID) -> None:
    """Test fetching playlist tracks."""
    client = TidlClient()

    logger.info("Authenticating...")
    if not client.authenticate():
        logger.error("Authentication failed. Cannot proceed with playlist test.")
        return

    session = client.session
    playlist_service = PlaylistService(session)
    # track_service = TrackService(session)

    playlist: Playlist = playlist_service.get_playlist(playlist_id)
    tracks: list[Track] = playlist_service.get_playlist_tracks(playlist)
    for track in tracks:
        if not track.available:
            logger.warning("Track {} is not available", track.name)
        logger.info("Track: {}, is available as {}, duration: {} s", track.name, track.audio_quality, track.duration)


if __name__ == "__main__":
    playlist_test()
