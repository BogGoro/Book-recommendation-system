from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from jose import JWTError

from src import middlewares


def build_app_with_refresh():
    app = FastAPI()
    app.middleware("http")(middlewares.refresh)

    @app.get("/state")
    async def state(request: Request):
        return {
            "access": getattr(request.state, "access_token", None),
            "refresh": getattr(request.state, "refresh_token", None),
        }

    return app


def test_refresh_passes_through_without_cookies():
    app = build_app_with_refresh()
    client = TestClient(app)

    r = client.get("/state")
    assert r.status_code == 200
    assert r.json() == {"access": None, "refresh": None}


def test_refresh_rotates_tokens_when_access_expired(monkeypatch):
    app = build_app_with_refresh()
    client = TestClient(app)

    monkeypatch.setattr(middlewares.jwt, "get_unverified_claims", lambda _: {"exp": 1})
    monkeypatch.setattr(middlewares.time, "time", lambda: 999999)
    monkeypatch.setattr(middlewares.auth, "refresh_tokens", lambda _: ("new-access", "new-refresh"))

    r = client.get("/state", cookies={"access": "old-access", "refresh": "old-refresh"})
    assert r.status_code == 200
    assert r.json() == {"access": "new-access", "refresh": "new-refresh"}
    assert "access=new-access" in r.headers.get("set-cookie", "") or "refresh=new-refresh" in r.headers.get("set-cookie", "")


def test_refresh_clears_cookies_on_invalid_access(monkeypatch):
    app = build_app_with_refresh()
    client = TestClient(app)

    def raise_jwt(_):
        raise JWTError("broken token")

    monkeypatch.setattr(middlewares.jwt, "get_unverified_claims", raise_jwt)

    r = client.get("/state", cookies={"access": "bad", "refresh": "any"})
    assert r.status_code == 200
    assert r.json() == {"access": None, "refresh": None}


def test_authorization_middleware_redirects_unauthorized(monkeypatch):
    app = FastAPI()
    app.middleware("http")(middlewares.make_authorization_middleware(["/private"]))

    @app.get("/private")
    async def private():
        return JSONResponse({"ok": True})

    monkeypatch.setattr(middlewares.auth, "get_user_data", lambda _: {})
    client = TestClient(app)

    r = client.get("/private", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/signin"


def test_authorization_middleware_allows_authorized(monkeypatch):
    app = FastAPI()
    app.middleware("http")(middlewares.make_authorization_middleware(["/private"]))

    @app.get("/private")
    async def private():
        return JSONResponse({"ok": True})

    monkeypatch.setattr(middlewares.auth, "get_user_data", lambda _: {"preferred_username": "Denis"})
    client = TestClient(app)

    r = client.get("/private")
    assert r.status_code == 200
    assert r.json() == {"ok": True}