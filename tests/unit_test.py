import requests
from time import sleep
from src.config import TEST_BACKEND_LOGIN, TEST_BACKEND_PASSWORD


def test_backend_healthy():
    response = requests.get("http://backend:8000/api/healthchecker")
    assert response.status_code == 200
    assert response.json()["message"] == "Healthy"


def test_root_endpoint():
    response = requests.get("http://backend:8000")
    assert response.status_code == 200


def test_catalog_endpoint():
    response = requests.get("http://backend:8000/catalog")
    assert response.status_code == 200


def test_book_endpoint():
    response = requests.get("http://backend:8000/book?id=1")
    assert response.status_code == 200


def test_registration_endpoint():
    response = requests.get("http://backend:8000/registration")
    assert response.status_code == 200


def test_backend_auth():
    session = requests.Session()
    reg = session.post(
        "http://backend:8000/registration",
        data={
            "username": TEST_BACKEND_LOGIN,
            "email": "test@example.com",
            "password": TEST_BACKEND_PASSWORD,
            "password_confirm": TEST_BACKEND_PASSWORD,
        },
        allow_redirects=False,
    )
    if reg.status_code not in (303, 307):
        sign = session.post(
            "http://backend:8000/signin",
            data={
                "username": TEST_BACKEND_LOGIN,
                "password": TEST_BACKEND_PASSWORD,
            },
            allow_redirects=False,
        )
        assert sign.status_code in (303, 307)
    else:
        assert reg.status_code in (303, 307)


def test_airflow_healthy():
    sleep(15)
    response = requests.get("http://airflow-webserver:8080/health")
    assert response.status_code == 200


def test_postgres():
    from src.scripts.pg_connect import PgConnectionBuilder

    connection = PgConnectionBuilder.pg_conn()

    with connection.connection() as conn:
        assert conn
