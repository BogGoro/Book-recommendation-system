import os


# src/config.py reads required vars at import time.
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "test_db")
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("INTERNAL_POSTGRES_PORT", "5432")

os.environ.setdefault("CLICKHOUSE_HOST", "localhost")
os.environ.setdefault("CLICKHOUSE_PORT", "9000")
os.environ.setdefault("CLICKHOUSE_DB", "test_db")
os.environ.setdefault("CLICKHOUSE_USER", "default")
os.environ.setdefault("CLICKHOUSE_PASSWORD", "test_password")
os.environ.setdefault("INTERNAL_CLICKHOUSE_PORT", "9000")

os.environ.setdefault("TEST_BACKEND_LOGIN", "test_login")
os.environ.setdefault("TEST_BACKEND_PASSWORD", "test_password")
