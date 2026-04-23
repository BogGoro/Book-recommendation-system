import logging
from lib.ch_connect import CHConnect


class AnalyticsRefresher:
    """
    Handles atomic refresh of analytical tables in ClickHouse.

    This class implements the 4-step process:
    1. Create temporary tables
    2. Populate with fresh data
    3. Atomic swap using RENAME TABLE
    4. Clean up old tables
    """

    def __init__(self, ch_conn: CHConnect, log: logging.Logger):
        """
        Initialize AnalyticsRefresher.

        Args:
            ch_conn: ClickHouse connection wrapper
            log: Logger instance
        """
        self.ch_conn = ch_conn
        self.log = log

    def create_temp_tables(self, conn) -> None:
        """Step 1: Create temporary tables from existing ones."""
        self.log.info("Creating temporary tables...")

        conn.execute("CREATE TABLE IF NOT EXISTS PersonalPart_new AS PersonalPart")
        conn.execute("CREATE TABLE IF NOT EXISTS Top_new AS Top")
        conn.execute("CREATE TABLE IF NOT EXISTS WeeklyTop_new AS WeeklyTop")
        conn.execute("CREATE TABLE IF NOT EXISTS Recommendations_new AS Recommendations")
        self.log.info("Temporary tables created successfully")

    def populate_personal_part(self, conn) -> int:
        """Step 2: Populate PersonalPart table with fresh data."""
        self.log.info("Populating PersonalPart...")

        result = conn.execute(
            """
            INSERT INTO PersonalPart_new
            SELECT
                u.id AS userid,
                b.id AS bookid,
                (
                    COALESCE(AVG(ts.score), 0) * COALESCE(AVG(ts.votes), 0) + 
                    COALESCE(AVG(gs.score), 0) * COALESCE(AVG(gs.votes), 0) + 
                    COALESCE(AVG(tgs.score), 0) * COALESCE(AVG(tgs.votes), 0)
                ) / 3 AS compatibility,
                now() AS createts
            FROM User u
            CROSS JOIN Book b
            LEFT JOIN BookType bt ON b.id = bt.bookid
            LEFT JOIN BookGenre bg ON b.id = bg.bookid
            LEFT JOIN BookTag btg ON b.id = btg.bookid
            LEFT JOIN TypeScore ts ON u.id = ts.userid AND bt.typeid = ts.typeid
            LEFT JOIN GenreScore gs ON u.id = gs.userid AND bg.genreid = gs.genreid
            LEFT JOIN TagScore tgs ON u.id = tgs.userid AND btg.tagid = tgs.tagid
            GROUP BY u.id, b.id
        """
        )

        # Get row count from result
        row_count = result[0][0] if result and len(result) > 0 else 0
        self.log.info(f"PersonalPart populated with {row_count} rows")
        return row_count

    def populate_top(self, conn) -> int:
        """Step 2: Populate Top table with fresh data."""
        self.log.info("Populating Top...")

        result = conn.execute(
            """
            INSERT INTO Top_new
            WITH scores AS (
                SELECT
                    b.id AS bookid,
                    AVG(s.score) AS score,
                    COUNT(s.score) AS votes
                FROM Book b
                LEFT JOIN Score s ON b.id = s.bookid AND s.score > 0
                GROUP BY b.id
            ),
            normalized AS (
                SELECT
                    s.bookid,
                    COALESCE(
                        s.score / NULLIF((SELECT MAX(score) FROM scores), 0),
                        0
                    ) AS normscore,
                    COALESCE(
                        CAST(s.votes AS Float32) / NULLIF((SELECT MAX(votes) FROM scores), 0),
                        0
                    ) AS normvotes
                FROM scores s
            )
            SELECT
                n.bookid AS bookid,
                ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) AS rank,
                n.normscore * n.normvotes AS compscore,
                now() AS createts
            FROM normalized n
        """
        )

        row_count = result[0][0] if result and len(result) > 0 else 0
        self.log.info(f"Top populated with {row_count} rows")
        return row_count

    def populate_weekly_top(self, conn) -> int:
        """Step 2: Populate WeeklyTop table with fresh data."""
        self.log.info("Populating WeeklyTop...")

        result = conn.execute(
            """
            INSERT INTO WeeklyTop_new
            WITH weekscores AS (
                SELECT
                    b.id AS bookid,
                    AVG(s.score) AS score,
                    COUNT(s.score) AS votes
                FROM Book b
                LEFT JOIN Score s ON b.id = s.bookid
                WHERE s.score > 0
                    AND s.createts >= NOW() - INTERVAL 7 DAY
                GROUP BY b.id
            ),
            normalized AS (
                SELECT
                    s.bookid,
                    COALESCE(
                        s.score / NULLIF((SELECT MAX(score) FROM weekscores), 0),
                        0
                    ) AS normscore,
                    COALESCE(
                        CAST(s.votes AS Float32) / NULLIF((SELECT MAX(votes) FROM weekscores), 0),
                        0
                    ) AS normvotes
                FROM weekscores s
            )
            SELECT
                n.bookid AS bookid,
                ROW_NUMBER() OVER (ORDER BY n.normscore * n.normvotes DESC) AS rank,
                n.normscore * n.normvotes AS compscore,
                now() AS createts
            FROM normalized n
        """
        )

        row_count = result[0][0] if result and len(result) > 0 else 0
        self.log.info(f"WeeklyTop populated with {row_count} rows")
        return row_count

    def populate_recommendations(self, conn) -> int:
        """Step 2: Populate Recommendations table with fresh data."""
        self.log.info("Populating Recommendations...")

        result = conn.execute(
            """
            INSERT INTO Recommendations_new
            SELECT
                pp.userid,
                pp.bookid,
                ROW_NUMBER() OVER (
                    PARTITION BY pp.userid
                    ORDER BY COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0) DESC
                ) AS rank,
                (COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0)) AS compatibility,
                now() AS createts
            FROM PersonalPart pp
            INNER JOIN Top t ON t.bookid = pp.bookid
            WHERE (pp.userid, pp.bookid) NOT IN (
                SELECT userid, bookid
                FROM BookStatus
                WHERE status = 'completed'
            )
        """
        )

        row_count = result[0][0] if result and len(result) > 0 else 0
        self.log.info(f"Recommendations populated with {row_count} rows")
        return row_count

    def atomic_swap(self, conn) -> None:
        """Step 3: Atomic swap of tables using RENAME."""
        self.log.info("Performing atomic swap...")
        
        # Check if temp tables exist before swapping
        result = conn.execute("""
            SELECT count() 
            FROM system.tables 
            WHERE database = currentDatabase() 
            AND name IN ('PersonalPart_new', 'Top_new', 'WeeklyTop_new', 'Recommendations_new')
        """)
        
        if result[0][0] < 4:
            self.log.error("Temporary tables don't exist! Cannot perform atomic swap.")
            raise Exception("Temporary tables missing. Ensure create_temp_tables and populate steps completed successfully.")
        
        conn.execute("DROP TABLE IF EXISTS PersonalPart_old")
        conn.execute("DROP TABLE IF EXISTS Top_old")
        conn.execute("DROP TABLE IF EXISTS WeeklyTop_old")
        conn.execute("DROP TABLE IF EXISTS Recommendations_old")

        conn.execute(
            """
            RENAME TABLE
                PersonalPart TO PersonalPart_old,
                PersonalPart_new TO PersonalPart,
                Top TO Top_old,
                Top_new TO Top,
                WeeklyTop TO WeeklyTop_old,
                WeeklyTop_new TO WeeklyTop,
                Recommendations TO Recommendations_old,
                Recommendations_new TO Recommendations
        """
        )
        self.log.info("Atomic swap completed successfully")

    def cleanup(self, conn) -> None:
        """Step 4: Clean up old tables."""
        self.log.info("Cleaning up old tables...")

        conn.execute("DROP TABLE IF EXISTS PersonalPart_old")
        conn.execute("DROP TABLE IF EXISTS Top_old")
        conn.execute("DROP TABLE IF EXISTS WeeklyTop_old")
        conn.execute("DROP TABLE IF EXISTS Recommendations_old")
        self.log.info("Cleanup completed")
