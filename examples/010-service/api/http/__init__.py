from fastapi import FastAPI
from fastapi.applications import Lifespan

from .discovery import router as discovery_router
from .sessions import router as sessions_router


def build_fastapi_app(lifespan: Lifespan = None) -> FastAPI:
    app = FastAPI(
        title="ActionEngine HTTP Service",
        lifespan=lifespan,
        root_path="/api2",
        root_path_in_servers=True,
    )

    app.include_router(discovery_router, prefix="/discovery")
    app.include_router(sessions_router, prefix="/sessions")

    return app
