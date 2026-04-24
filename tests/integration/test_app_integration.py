from fastapi.testclient import TestClient

from src.microservices.recommendation_system_project import app
from src.routers import main
from src.scripts import auth
from src import middlewares


def test_healthchecker_endpoint_returns_healthy():
    client = TestClient(app)

    response = client.get("/api/healthchecker")

    assert response.status_code == 200
    assert response.json() == {"message": "Healthy"}


def test_protected_page_redirects_to_signin_for_anonymous_user():
    client = TestClient(app)

    response = client.get("/personal", follow_redirects=False)

    assert response.status_code in (302, 307)
    assert response.headers["location"] == "/signin"


def test_signin_then_open_personal_page(monkeypatch):
    class FakeUserList:
        def __init__(self, username: str):
            self.username = username

        def get_completed_list(self):
            return []

        def get_reading_list(self):
            return []

        def get_planned_list(self):
            return []

    class FakeUserStats:
        def __init__(self, username: str):
            self.username = username
            self.scores = [0, 0, 0, 0, 0]
            self.statuses = (0, 0, 0, 0)

    def fake_get_user_data(request):
        access = getattr(request.state, "access_token", None)
        if access == "acc":
            return {"preferred_username": "denis"}
        return {}

    monkeypatch.setattr(auth, "authenticate", lambda username, password: ("acc", "ref"))
    monkeypatch.setattr(auth, "get_user_data", fake_get_user_data)
    monkeypatch.setattr(
        middlewares.jwt,
        "get_unverified_claims",
        lambda token: {"exp": 9999999999},
    )
    monkeypatch.setattr(main, "UserList", FakeUserList)
    monkeypatch.setattr(main, "UserStats", FakeUserStats)

    client = TestClient(app)

    signin = client.post(
        "/signin",
        data={"username": "denis", "password": "pass"},
        follow_redirects=False,
    )
    assert signin.status_code in (302, 303, 307)
    assert signin.headers["location"] == "/personal"

    personal = client.get("/personal")
    assert personal.status_code == 200
