import os

import uvicorn
import fastapi
from fastapi.responses import Response

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.routers.main import router
from src.routers.feedback import router as score_router
from src.routers.api import router as api_router
from src import middlewares

# fastapi application
app = fastapi.FastAPI( title="Reccomendation System backend API",
    description="This is an API for our Books recommendation system for capstone project in Innopolis University.\n \
          In our stack we use FastAPI+HTML+CSS+JS+Jinja2 for backend&frontend, JWT authentication backed by PostgreSQL, Airflow+ClickHouse for data processing and PostgreSQL for data storing. ",
    contact={
        "Github": "https://github.com/IU-Capstone-Project-2025/Recommendation-System"
    },
    license_info={
        "name": "MIT",
    },
)

# inject middlewares
app.middleware("http")(
    middlewares.make_authorization_middleware(["/personal", "/api/personal"])
)
app.middleware("http")(middlewares.refresh)
app.add_middleware(middlewares.MetricsMiddleware)

# include routers
app.include_router(router, tags=["Main"])
app.include_router(score_router, prefix="/api", tags=["Score"])
app.include_router(api_router, tags=["API"])



@app.get("/api/healthchecker",
 description="This is a health checker handler for our application.\
     We use it to check if our application is up and running during deployment.",
    tags=["Health checker"],
    responses={
        200: {"description": "Successful response. Service is up"},
        404: {"description": "Service is down"},
    }
)
def root():
    return {"message": "Healthy"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def start():
    reload = os.environ.get("UVICORN_RELOAD", "0").strip() in ("1", "true", "yes")
    uvicorn.run(
        "src.microservices.recommendation_system_project:app",
        host="0.0.0.0",
        port=8000,
        reload=reload,
        timeout_graceful_shutdown=10000000,
    )


if __name__ == "__main__":
    start()
