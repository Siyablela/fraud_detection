"""ASGI entry point: ``uvicorn app.main:app``. All the wiring lives in ``app.api.create_app``."""

from app.api import create_app

app = create_app()

__all__ = ["app", "create_app"]
