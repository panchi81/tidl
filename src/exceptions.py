class TidlError(Exception):
    """Base exception for TIDAL-related errors."""


class AuthError(TidlError):
    """Authentication related errors."""


class APIError(TidlError):
    """API call related errors."""


class PlaylistError(TidlError):
    """Playlist related errors."""


class TrackError(TidlError):
    """Track related errors."""
