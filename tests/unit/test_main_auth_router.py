from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routers import main
from src.scripts.exceptions import BadCredentials, UsernameNotUnique


def build_app():
    app = FastAPI()
    app.include_router(main.router)
    return app


def test_signin_bad_credentials(monkeypatch):
    app = build_app()
    client = TestClient(app)

    def bad_auth(username, password):
        raise BadCredentials

    monkeypatch.setattr(main.auth, "authenticate", bad_auth)

    r = client.post("/signin", data={"username": "Denis", "password": "wrong"})
    assert r.status_code == 401
    assert "wrong password or username" in r.text


def test_signin_success_sets_cookies(monkeypatch):
    app = build_app()
    client = TestClient(app)

    monkeypatch.setattr(main.auth, "authenticate", lambda username, password: ("acc", "ref"))

    r = client.post(
        "/signin",
        data={"username": "Denis", "password": "pass"},
        follow_redirects=False,
    )
    assert r.status_code in (302, 303, 307)
    assert r.headers["location"] == "/personal"
    assert "set-cookie" in r.headers


def test_registration_password_mismatch(monkeypatch):
    app = build_app()
    client = TestClient(app)

    r = client.post(
        "/registration",
        data={
            "username": "Denis",
            "email": "Denis@example.com",
            "password": "p1",
            "password_confirm": "p2",
        },
    )
    assert r.status_code == 400
    assert "passwords don&#39;t match" in r.text or "passwords don't match" in r.text


def test_registration_username_not_unique(monkeypatch):
    app = build_app()
    client = TestClient(app)

    def fail_create_user(username, password, email):
        raise UsernameNotUnique

    monkeypatch.setattr(main.auth, "create_user", fail_create_user)

    r = client.post(
        "/registration",
        data={
            "username": "Denis",
            "email": "Denis@example.com",
            "password": "pass",
            "password_confirm": "pass",
        },
    )
    assert r.status_code == 409
    assert "already taken" in r.text


def test_registration_success_redirects_to_personal(monkeypatch):
    app = build_app()
    client = TestClient(app)

    monkeypatch.setattr(main.auth, "create_user", lambda username, password, email: None)
    monkeypatch.setattr(main.auth, "authenticate", lambda username, password: ("acc", "ref"))

    r = client.post(
        "/registration",
        data={
            "username": "Denis",
            "email": "Denis@example.com",
            "password": "pass",
            "password_confirm": "pass",
        },
        follow_redirects=False,
    )
    assert r.status_code in (302, 303, 307)
    assert r.headers["location"] == "/personal"