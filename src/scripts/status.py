from src.scripts.pg_connect import PgConnectionBuilder
from src.scripts.exceptions import ObjectNotFound
from src.constants import COMPLETED, READING, PLANNED


class Status:
    username: str
    bookId: int
    newstatus: str | None
    status: str | None
    userid: int

    def __init__(self, username: str, bookId: int, status: str | None = None):
        self.username = username
        self.bookId = bookId
        self.newstatus = status
        self._db = PgConnectionBuilder.pg_conn()
        self.userid = self.get_userid()
        self.status = self.get_status()

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

    def get_status(self) -> str | None:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT status
                    FROM status
                    WHERE userid = %(userid)s AND bookid = %(bookid)s
                """,
                {"userid": self.userid, "bookid": self.bookId},
            )

            res = cur.fetchone()
            if not res:
                return None

            return res[0]

    def set_status(self) -> None:
        if self.newstatus not in [COMPLETED, READING, PLANNED]:
            return
        client = self._db.client()
        with client.cursor() as cur:
            if self.get_status() is not None:
                cur.execute(
                    f"DELETE FROM bookstatus WHERE userid = %(userid)s AND bookid = %(bookid)s",
                    {"userid": self.userid, "bookid": self.bookId},
                )
            cur.execute(
                f"INSERT INTO bookstatus (userid, bookid, status) VALUES (%(userid)s, %(bookid)s, %(status)s)",
                {
                    "userid": self.userid,
                    "bookid": self.bookId,
                    "status": self.newstatus,
                },
            )
            client.commit()

    def drop_status(self) -> None:
        if self.status in [COMPLETED, READING, PLANNED]:
            client = self._db.client()
            with client.cursor() as cur:
                cur.execute(
                    f"DELETE FROM bookstatus WHERE userid = %(userid)s AND bookid = %(bookid)s",
                    {"userid": self.userid, "bookid": self.bookId},
                )
                cur.execute(
                f"INSERT INTO bookstatus (userid, bookid, status) VALUES (%(userid)s, %(bookid)s, 'deleted')",
                    {"userid": self.userid, "bookid": self.bookId},
                )
                client.commit()
