import logging
from datetime import datetime

from airflow.decorators import dag, task
from lib.pg_connect import PgConnectionBuilder
from lib.ch_connect import CHConnectionBuilder
from tables.user_loader import UserLoader
from tables.book_loader import BookLoader
from tables.type_loader import BookTypeLoader
from tables.genre_loader import GenreLoader
from tables.tag_loader import BookTagLoader
from tables.score_loader import BookRatingLoader
from tables.bookstatus_loader import BookStatusesLoader
from tables.message_loader import MessageLoader
from tables.bookgenre_loader import BookGenreLoader
from tables.booktag_loader import BookTagLoader
from tables.booktype_loader import BookTypeLoader
from tables.analytics_refresher import AnalyticsRefresher

# Initialize logger for the DAG
log = logging.getLogger(__name__)


@dag(
    schedule_interval="* * * * *",  # Run every minute
    start_date=datetime(2025, 6, 11),  # Initial execution date
    catchup=False,  # Disable catchup to prevent backfilling
    tags=["stg", "postgres", "clickhouse"],  # Organizational tags
    is_paused_upon_creation=False,  # Start DAG immediately upon creation
)
def stg_dag():
    """
    Airflow DAG for loading data from PostgreSQL to ClickHouse (staging layer).

    This DAG orchestrates the parallel loading of multiple data entities:
    - Core entities: Users, Books, Types, Genres, Tags
    - User activity: Scores, Book statuses
    - Relationships: Book-Genre, Book-Tag, Book-Type mappings
    - User messages

    The DAG uses separate tasks for each entity to enable parallel loading.
    """

    # Initialize database connections
    origin_pg_connect = PgConnectionBuilder.pg_conn(
        "POSTGRES_DEFAULT"
    )  # Source PostgreSQL connection
    dwh_clickhouse_connect = CHConnectionBuilder.ch_conn(
        "CLICKHOUSE_DEFAULT"
    )  # Target ClickHouse connection

    @task(task_id="users_load")
    def load_user():
        """Load user data from PostgreSQL to ClickHouse."""
        rest_loader = UserLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_users()
        return "users_loaded"

    @task(task_id="book_load")
    def load_book():
        """Load book data from PostgreSQL to ClickHouse."""
        rest_loader = BookLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book()
        return "books_loaded"

    @task(task_id="type_load")
    def load_type():
        """Load book type data from PostgreSQL to ClickHouse."""
        rest_loader = BookTypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_types()
        return "types_loaded"

    @task(task_id="genre_load")
    def load_genre():
        """Load genre data from PostgreSQL to ClickHouse."""
        rest_loader = GenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_genres()
        return "genres_loaded"

    @task(task_id="tag_load")
    def load_tag():
        """Load tag data from PostgreSQL to ClickHouse."""
        rest_loader = BookTagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_tags()
        return "tags_loaded"

    @task(task_id="score_load")
    def load_score():
        """Load user score data from PostgreSQL to ClickHouse."""
        rest_loader = BookRatingLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_ratings()
        return "scores_loaded"

    @task(task_id="book_status_load")
    def load_book_status():
        """Load book status data from PostgreSQL to ClickHouse."""
        rest_loader = BookStatusesLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_statuses()
        return "book_statuses_loaded"

    @task(task_id="message_load")
    def load_message():
        """Load user message data from PostgreSQL to ClickHouse."""
        rest_loader = MessageLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_messages()
        return "messages_loaded"

    @task(task_id="bookgenre_load")
    def load_bookgenre():
        """Load book-genre relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookGenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_genre()
        return "bookgenres_loaded"

    @task(task_id="booktag_load")
    def load_booktag():
        """Load book-tag relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookTagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_tags()
        return "booktags_loaded"

    @task(task_id="booktype_load")
    def load_booktype():
        """Load book-type relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookTypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_types()
        return "booktypes_loaded"

    # Analytics refresh tasks with atomic swap
    @task(task_id="create_temp_tables")
    def create_temp():
        """Create temporary tables for analytics."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            refresher.create_temp_tables(conn)
        return "temp_created"

    @task(task_id="populate_personal_part")
    def populate_personal_part():
        """Populate PersonalPart table."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            rows = refresher.populate_personal_part(conn)
        return rows

    @task(task_id="populate_top")
    def populate_top():
        """Populate Top table."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            rows = refresher.populate_top(conn)
        return rows

    @task(task_id="populate_weekly_top")
    def populate_weekly_top():
        """Populate WeeklyTop table."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            rows = refresher.populate_weekly_top(conn)
        return rows

    @task(task_id="populate_recommendations")
    def populate_recommendations():
        """Populate Recommendations table."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            rows = refresher.populate_recommendations(conn)
        return rows

    @task(task_id="atomic_swap")
    def atomic_swap(personal_rows, top_rows, weekly_rows, rec_rows):
        """Atomic swap all analytics tables."""
        log.info(
            f"Swapping tables with stats - PersonalPart: {personal_rows}, Top: {top_rows}, WeeklyTop: {weekly_rows}, Recommendations: {rec_rows}"
        )
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            refresher.atomic_swap(conn)
        return "swapped"

    @task(task_id="cleanup")
    def cleanup():
        """Clean up old tables."""
        refresher = AnalyticsRefresher(dwh_clickhouse_connect, log)
        with dwh_clickhouse_connect.connection() as conn:
            refresher.cleanup(conn)
        return "cleaned"

    # Define task dependencies by creating references to task outputs
    user_dict = load_user()
    book_dict = load_book()
    type_dict = load_type()
    genre_dict = load_genre()
    tag_dict = load_tag()
    score_dict = load_score()
    book_status_dict = load_book_status()
    message_dict = load_message()
    bookgenre_dict = load_bookgenre()
    booktag_dict = load_booktag()
    booktype_dict = load_booktype()
    temp = create_temp()
    personal = populate_personal_part()
    top = populate_top()
    weekly = populate_weekly_top()
    recommendations = populate_recommendations()
    swap = atomic_swap(personal, top, weekly, recommendations)
    clean = cleanup()

    # Return all task references to establish implicit dependencies
    # (Airflow will run all tasks in parallel where possible)
    (
        [
            user_dict,
            book_dict,
            type_dict,
            genre_dict,
            tag_dict,
            score_dict,
            book_status_dict,
            message_dict,
            bookgenre_dict,
            booktag_dict,
            booktype_dict,
        ]
        >> temp
        >> [personal, top, weekly]
        >> recommendations
        >> swap
        >> clean
    )


# Instantiate the DAG
stg_dag = stg_dag()
