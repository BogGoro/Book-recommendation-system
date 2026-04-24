import pytest
import bcrypt

from types import SimpleNamespace
from jose import JWTError

from src.scripts import auth
from src.scripts.exceptions import BadCredentials, UsernameNotUnique


def make_request_with_access_token(token):
    return SimpleNamespace(
        state=SimpleNamespace(
            access_token=token,
        )
    )

def test_authenticate_success(monkeypatch):
    password = "123qwerty"
    hashed = bcrypt.hashpw(
        password.encode(),
        bcrypt.gensalt(),
    ).decode()

    class FakeUser:
        def __init__(self, username):
            self.username = username
        
        def get_password_hash(self):
            return hashed

    monkeypatch.setattr(auth, "User", FakeUser)

    access, refresh = auth.authenticate("Denis", password)
    
    assert isinstance(access, str) and access
    assert isinstance(refresh, str) and refresh


def test_authenticate_bad_credentials_when_hash_missing(monkeypatch):
    class FakeUser:
        def __init__(self, username):
            self.username = username
        
        def get_password_hash(self):
            return None
    
    monkeypatch.setattr(auth, "User", FakeUser)

    with pytest.raises(BadCredentials):
        auth.authenticate("Denis", "something-wrong")

def test_get_user_data_empty_on_refresh_type(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda _: {
            "type": "refresh",
            "sub": "Denis",
        }
    )
    
    request = make_request_with_access_token("any-token")

    assert auth.get_user_data(request) == {}

def test_get_user_data_valid(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda _: {
            "preferred_username": "Denis",
            "type": "access",
        }
    )
    
    request = make_request_with_access_token("any-good-token")

    assert auth.get_user_data(request) == {"preferred_username": "Denis"}

def test_get_user_data_invalid_token(monkeypatch):
    def raise_jwt(_):
        raise JWTError()
    
    monkeypatch.setattr(auth, "_decode_token", raise_jwt)

    request = make_request_with_access_token("any-bad-token")

    assert auth.get_user_data(request) == {}

def test_refresh_tokens_raises_when_not_refresh(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda _: {
            "sub": "Denis",
            "type": "access",
        }
    )
    
    with pytest.raises(ValueError, match="not a refresh token"):
        auth.refresh_tokens("token")

def test_refresh_tokens_raises_when_unknown_user(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda _: {
            "type": "refresh",
            "sub": "ghost-user",
        }
    )

    class FakeUser:
        def __init__(self, username):
            self.username = username
        
        def get_id(self):
            return None
    
    monkeypatch.setattr(auth, "User", FakeUser)
    
    with pytest.raises(ValueError, match="unknown user"):
        auth.refresh_tokens("token")

def test_refresh_tokens_success(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda _: {
            "type": "refresh",
            "sub": "Denis",
        }
    )
    
    class FakeUser:
        def __init__(self, username):
            self.username = username

        def get_id(self):
            return 1

    monkeypatch.setattr(auth, "User", FakeUser)
    monkeypatch.setattr(auth, "_create_access_token", lambda username: f"acc:{username}")
    monkeypatch.setattr(auth, "_create_refresh_token", lambda username: f"ref:{username}")

    access, refresh = auth.refresh_tokens("token")

    assert access == "acc:Denis"
    assert refresh == "ref:Denis"

def test_create_user_propagates_username_not_unique(monkeypatch):
    class FakeUser:
        def __init__(self, username):
            self.username = username
        
        def insert(self, password_hash, email):
            raise UsernameNotUnique
    
    monkeypatch.setattr(auth, "User", FakeUser)

    with pytest.raises(UsernameNotUnique):
        auth.create_user("Denis", "123qwerty", "denis@example.com")