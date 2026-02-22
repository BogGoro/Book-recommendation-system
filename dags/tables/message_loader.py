from logging import Logger
from typing import List, Tuple, Any
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from pydantic import BaseModel
from lib.pg_connect import PgConnect
from lib.ch_connect import CHConnect


class Message(BaseModel):
    """
    Data model representing a user message with validation.

    Attributes:
        id: Unique message identifier
        version: Action number (for incremental loading)
        userid: ID of the user who sent the message
        bookid: ID of the book the message relates to
        message: Content of the message
        status: Status of the message (created, updated, deleted)
        createts: Timestamp of creation
    """

    id: int
    version: int
    userid: int
    bookid: int
    message: str
    status: str
    createts: datetime

    @classmethod
    def from_dict(cls, data: Tuple[Any]) -> "Message":
        """
        Create Message from database tuple.

        Args:
            data: Tuple containing (id, version, userid, bookid, message, status, createts)

        Returns:
            Message: Validated message object
        """
        return cls(
            id=data[0],
            version=data[1],
            userid=data[2],
            bookid=data[3],
            message=data[4],
            status=data[5],
            createts=data[6],
        )


class MessageRepository:
    """
    Repository for retrieving message data from PostgreSQL.
    Handles batched fetching with incremental loading support.
    """

    def __init__(self, pg: PgConnect) -> None:
        """
        Initialize with PostgreSQL connection.

        Args:
            pg: Configured PostgreSQL connection wrapper
        """
        self._db = pg

    def get_max_version(self) -> int:
        """
        Get the maximum version of the book rating table.

        Returns:
            int: Maximum version
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT MAX(version) 
                FROM message
                """
            )
            result = cur.fetchone()
            return result[0] if result else -1

    def list_messages(
        self, threshold: int, target: int, batch_size: int = 10000
    ) -> List[Message]:
        """
        Get batch of messages updated after threshold.

        Args:
            threshold: Minimum version to include
            target: Maximum version to include
            batch_size: Number of records per batch (default: 10,000)

        Returns:
            List[Message]: Batch of message objects
        """
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, version, userid, bookid, message, status, createts
                FROM message
                WHERE version > %(threshold)s AND version <= %(target)s
                ORDER BY version ASC
                LIMIT %(batch_size)s
                """,
                {"threshold": threshold, "target": target, "batch_size": batch_size},
            )
            rows = cur.fetchall()
        return [Message.from_dict(row) for row in rows]


class MessageDestinationRepository:
    """
    Repository for loading message data into ClickHouse.
    Optimized for efficient batch inserts of message content.
    """

    def insert_batch(self, conn: ClickhouseClient, messages: List[Message]) -> None:
        """
        Insert batch of messages into ClickHouse.

        Args:
            conn: Active ClickHouse connection
            messages: List of message objects to insert

        Note:
            Silently returns if input list is empty
            Converts boolean isactual to ClickHouse-compatible UInt8
        """
        if not messages:
            return

        # Convert to ClickHouse-compatible format
        data = [
            [
                msg.id,
                msg.version,
                msg.userid,
                msg.bookid,
                msg.message,
                msg.status,
                msg.createts,
            ]
            for msg in messages
        ]

        conn.execute(
            """
            INSERT INTO Message (id, version, userid, bookid, message, status, createts) VALUES
            """,
            data,
        )


class MessageLoader:
    """
    Orchestrates the complete ETL process for message data.
    Implements incremental loading with progress tracking.
    """

    BATCH_SIZE = 10000  # Optimal batch size for bulk operations

    def __init__(self, pg_origin: PgConnect, ch_dest: CHConnect, log: Logger) -> None:
        """
        Initialize loader with connections and logger.

        Args:
            pg_origin: Source PostgreSQL connection
            ch_dest: Target ClickHouse connection
            log: Logger instance for progress tracking
        """
        self.ch_dest = ch_dest
        self.origin = MessageRepository(pg_origin)
        self.stg = MessageDestinationRepository()
        self.log = log

    def load_messages(self) -> None:
        """
        Execute complete loading process:
        1. Gets last loaded version from target
        2. Fetches batches from source updated since last load
        3. Inserts batches into target
        4. Repeats until all updates processed
        5. Logs progress and completion
        """
        with self.ch_dest.connection() as conn:
            # Get most recent update from target
            last_loaded = conn.execute("SELECT MAX(version) FROM Message")[0][0]
            if not last_loaded:
                last_loaded = -1  # Initial load marker

            target_version = self.origin.get_max_version()

            if target_version <= last_loaded:
                self.log.info("No new messages to load")
                return

            total_loaded = 0
            batch_num = 1

            # Process batches until completion
            while last_loaded != target_version:
                batch = self.origin.list_messages(last_loaded, target_version, self.BATCH_SIZE)
                self.log.info(f"Batch {batch_num}: {len(batch)} messages to load")
                if not batch:
                    break
                
                self.stg.insert_batch(conn, batch)

                # Update counters for next batch
                total_loaded += len(batch)
                last_loaded = batch[-1].version
                batch_num += 1

            # Optimize table
            conn.execute("OPTIMIZE TABLE Message FINAL")

            self.log.info(f"Load complete. Total messages loaded: {total_loaded}")
