"""Optional local group robot and its authoritative business services."""

from .store import BotStore
from .service import Service

__all__ = ["BotStore", "Service"]
