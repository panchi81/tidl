from loguru import logger
from tidalapi import Session
from tidalapi.media import Track
from tidalapi.playlist import Playlist

from src.exceptions import PlaylistError, StreamInfoError, TrackError
from src.stream_info import StreamInfo


class PlaylistService:
    """Handle playlist operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_playlist(self, playlist_id: str) -> Playlist:
        """Get a playlist object by ID."""
        try:
            playlist = self.session.playlist(playlist_id)
        except PlaylistError as e:
            msg = f"Failed to get playlist with ID: {playlist_id}"
            logger.exception(msg)
            raise PlaylistError(msg) from e
        else:
            logger.info("Fetched playlist: {} with {} tracks", playlist.name, playlist.num_tracks)
            return playlist

    def get_playlist_tracks(self, playlist: Playlist) -> list[Track]:
        """Get tracks from a playlist."""
        try:
            tracks = playlist.tracks()
        except PlaylistError as e:
            msg = f"Failed to fetch tracks for playlist: {playlist.name}"
            logger.exception(msg)
            raise PlaylistError(msg) from e
        else:
            logger.info("Fetched {} tracks from playlist: {}", len(tracks), playlist.name)
            return tracks


class TrackService:
    """Handle track operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_track(self, track_id: int) -> Track:
        """Get a track object by ID."""
        try:
            track = self.session.track(track_id)
        except TrackError as e:
            msg = f"Failed to get track with ID: {track_id}"
            logger.exception(msg)
            raise TrackError(msg) from e
        else:
            logger.info("Fetched track: {} - {}", track.artist, track.full_name)
            return track

    def get_track_safe_name(self, track: Track) -> str:
        """Generate a safe filename for a track."""
        safe_name = f"{track.artist.name} - {track.name}".replace("/", "_").replace("\\", "_")
        logger.debug("Generated safe filename: {}", safe_name)
        return safe_name

    def get_stream_info(self, track: Track) -> StreamInfo:
        """Get stream information for a track."""
        try:
            return StreamInfo.from_track(track)
        except StreamInfoError as e:
            msg = f"Failed to get stream info for track: {track.title}"
            logger.exception(msg)
            raise StreamInfoError(msg) from e
