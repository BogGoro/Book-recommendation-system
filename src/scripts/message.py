from src.scripts.exceptions import ObjectNotFound
from src.scripts.pg_connect import PgConnectionBuilder


class Message:
    def __init__(self, username: str, bookId: int, message: str, id: int | None = None):
        self.username = username
        self.bookId = bookId
        self.id = id
        self._db = PgConnectionBuilder.pg_conn()
        self.message = message
        self.userid = self.get_userid()

    def get_userid(self) -> int:
        with self._db.client().cursor() as cur:
            cur.execute(
                'SELECT id FROM "User" WHERE username = %(username)s',
                {"username": self.username},
            )

            res = cur.fetchone()
            if not res:
                raise ObjectNotFound

            return res[0]

    def set_message(self) -> None:
        client = self._db.client()
        with client.cursor() as cur:
            cur.execute(
                """
                        INSERT INTO message (userid, bookid, message, status)
                        VALUES (%(userid)s, %(bookid)s, %(message)s, 'created')
                        """,
                {"userid": self.userid, "bookid": self.bookId, "message": self.message},
            )
            client.commit()

    def edit_message(self) -> None:
        client = self._db.client()
        with client.cursor() as cur:
            cur.execute(
                """
                        DELETE FROM message WHERE id = %(id)s
                        """,
                {
                    "id": self.id,
                },
            )
            cur.execute(
                """
                        INSERT INTO message (id, userid, bookid, message, status)
                        VALUES (%(id)s, %(userid)s, %(bookid)s, %(message)s, 'edited')
                        """,
                {
                    "id": self.id,
                    "userid": self.userid,
                    "bookid": self.bookId,
                    "message": self.message,
                },
            )
            client.commit()

    def delete_message(self) -> None:
        client = self._db.client()
        with client.cursor() as cur:
            cur.execute(
                """
                        DELETE FROM message WHERE id = %(id)s
                        """,
                {
                    "id": self.id,
                },
            )
            cur.execute(
                """
                        INSERT INTO message (id, userid, bookid, message, status)
                        VALUES (%(id)s, %(userid)s, %(bookid)s, '', 'deleted')
                        """,
                {
                    "id": self.id,
                    "userid": self.userid,
                    "bookid": self.bookId,
                },
            )
            client.commit()
