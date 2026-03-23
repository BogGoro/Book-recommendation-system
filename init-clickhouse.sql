-- Core User Table
CREATE TABLE IF NOT EXISTS User (
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

-- analysis (will be updated via atomic swap)
CREATE TABLE IF NOT EXISTS PersonalPart (
    userid UInt32,
    bookid UInt32,
    compatibility Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (userid, bookid);

CREATE TABLE IF NOT EXISTS Top (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid);

CREATE TABLE IF NOT EXISTS WeeklyTop (
    bookid UInt32,
    rank UInt32,
    compscore Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (bookid);

CREATE TABLE IF NOT EXISTS Recommendations (
    userid UInt32,
    bookid UInt32,
    rank UInt32,
    compatibility Float32,
    createts DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(createts)
ORDER BY
    (userid, bookid);
