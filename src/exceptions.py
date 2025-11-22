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


class DownloadError(TidlError):
    """Download related errors."""


class StreamInfoError(TidlError):
    """Stream information related errors."""


class InterruptError(TidlError):
    """Operation interrupted by user."""


class MetadataError(TidlError):
    """Metadata writing related errors."""


class CoverImageError(TidlError):
    """Cover image related errors."""


class DBError(Exception):
    """Database related errors."""
