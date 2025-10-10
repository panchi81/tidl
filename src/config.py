from pathlib import Path
from typing import ClassVar


class SingletonMeta(type):
    """A metaclass for singleton classes."""

    _instances: ClassVar[dict] = {}

    def __call__(cls, *args, **kwargs) -> object:
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class BaseConfig:
    file_path: Path
