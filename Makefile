# Plain progress: full build log
.PHONY: build-backend up-stack logs-backend dev-hot

build-backend:
	DOCKER_BUILDKIT=1 docker compose --progress=plain build backend

# DB + backend
up-stack:
	docker compose up -d postgres clickhouse backend

logs-backend:
	docker compose logs -f --tail=100 backend

# Uvicorn --reload
dev-hot:
	UVICORN_RELOAD=1 docker compose up -d --force-recreate backend
