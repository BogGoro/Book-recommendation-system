from psycopg2 import IntegrityError
from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import UsernameNotUnique


class User:
    username: str

    def __init__(self, username: str):
        self.username = username
        self._db = PgConnectionBuilder.pg_conn()

    def get_id(self) -> int | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]

    def get_password_hash(self) -> str | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT password_hash FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def insert(self, password_hash: str, email: str | None) -> None:
        client = self._db.client()
        with client.cursor() as cur:
            try:
                cur.execute(
                    'INSERT INTO "User" (username, password_hash, email) VALUES (%(username)s, %(password_hash)s, %(email)s)',
                    {
                        "username": self.username,
                        "password_hash": password_hash,
                        "email": email,
                    },
                )
                client.commit()
            except IntegrityError:
                client.rollback()
                raise UsernameNotUnique from None
