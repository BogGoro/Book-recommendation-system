import time
from typing import Tuple

import bcrypt
from fastapi import Request
from jose import JWTError, jwt

from src import config
from src.scripts.exceptions import BadCredentials, UsernameNotUnique
from src.scripts.user import User


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt(),
    ).decode("utf-8")


def _verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(
        plain.encode("utf-8"),
        hashed.encode("utf-8"),
    )


def _create_access_token(username: str) -> str:
    now = int(time.time())
    exp = now + config.JWT_ACCESS_EXPIRE_MINUTES * 60
    payload = {
        "preferred_username": username,
        "iat": now,
        "exp": exp,
    }
    return jwt.encode(
        payload, config.JWT_SECRET_KEY, algorithm=config.JWT_ALGORITHM
    )


def _create_refresh_token(username: str) -> str:
    now = int(time.time())
    exp = now + config.JWT_REFRESH_EXPIRE_DAYS * 86400
    payload = {
        "sub": username,
        "type": "refresh",
        "iat": now,
        "exp": exp,
    }
    return jwt.encode(
        payload, config.JWT_SECRET_KEY, algorithm=config.JWT_ALGORITHM
    )


def _decode_token(token: str) -> dict:
    return jwt.decode(
        token,
        config.JWT_SECRET_KEY,
        algorithms=[config.JWT_ALGORITHM],
    )


def create_user(username: str, password: str, email: str):
    try:
        User(username).insert(_hash_password(password), email or None)
    except UsernameNotUnique:
        raise


def authenticate(username: str, password: str) -> Tuple[str, str]:
    user = User(username)
    ph = user.get_password_hash()
    if not ph or not _verify_password(password, ph):
        raise BadCredentials
    return _create_access_token(username), _create_refresh_token(username)


def get_user_data(request: Request) -> dict:
    try:
        access_token = request.state.access_token
    except AttributeError:
        return {}
    if not access_token:
        return {}
    try:
        claims = _decode_token(access_token)
        if claims.get("type") == "refresh":
            return {}
        username = claims.get("preferred_username")
        if not username:
            return {}
        return {"preferred_username": username}
    except JWTError:
        return {}


def refresh_tokens(refresh_token: str) -> Tuple[str, str]:
    claims = _decode_token(refresh_token)
    if claims.get("type") != "refresh":
        raise ValueError("not a refresh token")
    username = claims.get("sub")
    if not username:
        raise ValueError("missing subject")
    if User(username).get_id() is None:
        raise ValueError("unknown user")
    return _create_access_token(username), _create_refresh_token(username)


def logout(refresh: str):
    pass
