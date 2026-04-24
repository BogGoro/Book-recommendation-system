from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routers import feedback
from src.scripts.exceptions import ObjectNotFound


def build_app():
    app = FastAPI()
    app.include_router(feedback.router)
    return app


def test_feedback_returns_404_when_book_not_found(monkeypatch):
    app = build_app()
    client = TestClient(app)

    def fake_book(_):
        raise ObjectNotFound

    monkeypatch.setattr(feedback, "Book", fake_book)
    monkeypatch.setattr(feedback.auth, "get_user_data", lambda _: {"preferred_username": "Denis"})

    r = client.post("/feedback", data={"book_id": "1", "status": "completed"})
    assert r.status_code == 404
    assert r.text == "Book doesn't exist"


def test_feedback_returns_400_on_invalid_status(monkeypatch):
    app = build_app()
    client = TestClient(app)

    monkeypatch.setattr(feedback, "Book", lambda _: object())
    monkeypatch.setattr(feedback.auth, "get_user_data", lambda _: {"preferred_username": "Denis"})

    r = client.post("/feedback", data={"book_id": "1", "status": "invalid-status"})
    assert r.status_code == 400
    assert r.text == "Unknown book status"


def test_feedback_returns_400_on_invalid_score(monkeypatch):
    app = build_app()
    client = TestClient(app)

    monkeypatch.setattr(feedback, "Book", lambda _: object())
    monkeypatch.setattr(feedback.auth, "get_user_data", lambda _: {"preferred_username": "Denis"})

    r = client.post("/feedback", data={"book_id": "1", "score": "abc"})
    assert r.status_code == 400
    assert "score value should be integer from 1 to 5" in r.text


def test_feedback_score_zero_calls_drop_score(monkeypatch):
    app = build_app()
    client = TestClient(app)

    called = {"drop": False}

    class FakeScore:
        def __init__(self, username, book_id, score=None):
            self.username = username
            self.book_id = book_id
            self.score = score

        def drop_score(self):
            called["drop"] = True

        def set_score(self):
            pass

    monkeypatch.setattr(feedback, "Book", lambda _: object())
    monkeypatch.setattr(feedback, "Score", FakeScore)
    monkeypatch.setattr(feedback.auth, "get_user_data", lambda _: {"preferred_username": "Denis"})

    r = client.post("/feedback", data={"book_id": "1", "score": "0"})
    assert r.status_code == 200
    assert r.text == "OK"
    assert called["drop"] is True