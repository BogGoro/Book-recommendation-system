-- Core User Table
CREATE TABLE IF NOT EXISTS user (
    id UInt32,
    username String,
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS Book (
    id UInt32,
    title String,
    author String,
    year Nullable(Int32), -- Publication year (optional)
    imgurl Nullable(String), -- Book cover image URL (optional)
    description Nullable(String),
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS Genre (
    id UInt32,
    name String,
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS Tag (
    id UInt32,
    name String,
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (id);

CREATE TABLE IF NOT EXISTS TYPE (
    id UInt32,
    name String,
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (id);

-- User Engagement Tables

-- User's score for a book table
CREATE TABLE IF NOT EXISTS Score (
    userid UInt32,
    bookid UInt32,
    version UInt32,
    score Int32, -- User rating (0-5 stars) 0 = no rating
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS BookStatus (
    userid UInt32,
    bookid UInt32,
    version UInt32,
    status String,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS Message (
    id UInt32,
    version UInt32,
    userid UInt32,
    bookid UInt32,
    message String,
    status String,
    createts DateTime DEFAULT now(),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree(version)
ORDER BY
    (id);

-- relationship
CREATE TABLE IF NOT EXISTS BookGenre (
    bookid UInt32,
    genreid UInt32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid, genreid);

CREATE TABLE IF NOT EXISTS BookTag (
    bookid UInt32,
    tagid UInt32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid, tagid);

CREATE TABLE IF NOT EXISTS BookType (
    bookid UInt32,
    typeid UInt32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid, typeid);

-- analysis
/*
 TYPE/GENRE/TAG SCORE VIEW
 Calculates normalized user preference scores for book types, genres, and tags
 - Scores are normalized against each user's maximum values
 - Considers both average rating and number of votes
 */

CREATE VIEW IF NOT EXISTS TypeScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.typeid AS typeid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookType bt ON s.bookid = bt.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                t.id AS typeid
            FROM
                User u
                CROSS JOIN TYPE t
        ) ut ON u.id = ut.userid
        AND bt.typeid = ut.typeid
    WHERE
        s.score > 0
    GROUP BY
        ut.userid,
        ut.typeid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.typeid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS GenreScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.genreid AS genreid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookGenre bg ON s.bookid = bg.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                g.id AS genreid
            FROM
                User u
                CROSS JOIN Genre g
        ) ut ON u.id = ut.userid
        AND bg.genreid = ut.genreid
    WHERE
        s.score > 0
    GROUP BY
        ut.userid,
        ut.genreid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.genreid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

CREATE VIEW IF NOT EXISTS TagScore AS WITH scores AS (
    SELECT
        ut.userid AS userid,
        ut.tagid AS tagid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        User u
        INNER JOIN Score s ON u.id = s.userid
        INNER JOIN BookTag bt ON s.bookid = bt.bookid
        RIGHT JOIN (
            SELECT
                u.id AS userid,
                t.id AS tagid
            FROM
                User u
                CROSS JOIN Tag t
        ) ut ON u.id = ut.userid
        AND bt.tagid = ut.tagid
    WHERE
        s.score > 0
    GROUP BY
        ut.userid,
        ut.tagid
),
user_maxima AS (
    SELECT
        userid,
        MAX(score) AS max_score,
        MAX(votes) AS max_votes
    FROM
        scores
    GROUP BY
        userid
)
SELECT
    s.userid,
    s.tagid,
    IF(max_score > 0, score / max_score, 0) AS score,
    IF(max_votes > 0, votes / max_votes, 0) AS votes
FROM
    scores s
    JOIN user_maxima um ON s.userid = um.userid;

-- analysis
CREATE TABLE IF NOT EXISTS PersonalPart (
    userid UInt32,
    bookid UInt32,
    compatibility Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS PersonalPart_MV REFRESH EVERY 10 minutes REPLACE PersonalPart AS
SELECT
    u.id AS userid,
    b.id AS bookid,
    -- Combined compatibility score calculation
    (
        COALESCE(AVG(ts.score), 0) * COALESCE(AVG(ts.votes), 0) + COALESCE(AVG(gs.score), 0) * COALESCE(AVG(gs.votes), 0) + COALESCE(AVG(tgs.score), 0) * COALESCE(AVG(tgs.votes), 0)
    ) / 3 AS compatibility,
    now() AS createts
FROM
    User u
    CROSS JOIN Book b -- Consider all possible user-book combinations
    LEFT JOIN BookType bt ON b.id = bt.bookid
    LEFT JOIN BookGenre bg ON b.id = bg.bookid
    LEFT JOIN BookTag btg ON b.id = btg.bookid
    LEFT JOIN TypeScore ts ON u.id = ts.userid
    AND bt.typeid = ts.typeid
    LEFT JOIN GenreScore gs ON u.id = gs.userid
    AND bg.genreid = gs.genreid
    LEFT JOIN TagScore tgs ON u.id = tgs.userid
    AND btg.tagid = tgs.tagid
GROUP BY
    u.id,
    b.id;

CREATE TABLE IF NOT EXISTS Top (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS Top_MV REFRESH EVERY 10 minutes REPLACE Top AS WITH scores AS (
    SELECT
        b.id AS bookid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        Book b
        LEFT JOIN Score s ON b.id = s.bookid
        AND s.score > 0
    GROUP BY
        b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(
            s.score / NULLIF(
                (
                    SELECT
                        MAX(s.score) AS score
                    FROM
                        scores s
                ),
                0
            ),
            0
        ) AS normscore,
        COALESCE(
            s.votes :: FLOAT / NULLIF(
                (
                    SELECT
                        MAX(s.votes) AS votes
                    FROM
                        scores s
                ),
                0
            ),
            0
        ) AS normvotes
    FROM
        scores s
)
SELECT
    n.bookid AS bookid,
    ROW_NUMBER() OVER (
        ORDER BY
            n.normscore * n.normvotes DESC
    ) AS rank,
    n.normscore * n.normvotes AS compscore,
    now() AS createts
FROM
    normalized n;

CREATE TABLE IF NOT EXISTS WeeklyTop (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS WeeklyTop_MV REFRESH EVERY 10 minutes REPLACE WeeklyTop AS WITH weekscores AS (
    SELECT
        b.id AS bookid,
        AVG(s.score) AS score,
        COUNT(s.score) AS votes
    FROM
        Book b
        LEFT JOIN Score s ON b.id = s.bookid
    WHERE
        s.score > 0
        AND s.createts >= NOW() - INTERVAL '7' DAY
    GROUP BY
        b.id
),
normalized AS (
    SELECT
        s.bookid,
        COALESCE(
            s.score / NULLIF(
                (
                    SELECT
                        MAX(s.score) AS score
                    FROM
                        weekscores s
                ),
                0
            ),
            0
        ) AS normscore,
        COALESCE(
            s.votes :: FLOAT / NULLIF(
                (
                    SELECT
                        MAX(s.votes) AS votes
                    FROM
                        weekscores s
                ),
                0
            ),
            0
        ) AS normvotes
    FROM
        weekscores s
)
SELECT
    n.bookid AS bookid,
    ROW_NUMBER() OVER (
        ORDER BY
            n.normscore * n.normvotes DESC
    ) AS rank,
    n.normscore * n.normvotes AS compscore,
    now() AS createts
FROM
    normalized n;

CREATE TABLE IF NOT EXISTS Recommendations (
    userid UInt32,
    bookid UInt32,
    rank UInt32,
    compatibility Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (userid, bookid);

CREATE MATERIALIZED VIEW IF NOT EXISTS Recommendations_MV REFRESH EVERY 10 minutes REPLACE Recommendations AS
SELECT
    pp.userid,
    pp.bookid,
    ROW_NUMBER() OVER (
        PARTITION BY pp.userid
        ORDER BY
            COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0) DESC
    ) AS rank,
    (
        COALESCE(pp.compatibility, 0) + COALESCE(t.compscore, 0)
    ) AS compatibility,
    now() AS createts
FROM
    PersonalPart pp
    INNER JOIN Top t ON t.bookid = pp.bookid
WHERE
    (pp.userid, pp.bookid) NOT IN (
        SELECT
            userid,
            bookid
        FROM
            BookStatus
        WHERE
            status = 'completed'
    );