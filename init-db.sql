-- Core User Table
CREATE TABLE "User" (
    ID SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL, -- Unique username for login/display
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Book Catalog Table
CREATE TABLE Book (
    ID SERIAL PRIMARY KEY,
    title VARCHAR(100) UNIQUE NOT NULL,
    author VARCHAR(100),
    year INT, -- Publication year
    imgurl TEXT, -- URL for book cover image
    description TEXT,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Categorization Tables
CREATE TABLE Genre (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Tag (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE TYPE (
    ID SERIAL PRIMARY KEY,
    NAME VARCHAR(100) UNIQUE NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- User Engagement Tables

-- User's score for a book table
CREATE TABLE Score (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    version SERIAL NOT NULL,
    score INT NOT NULL, -- Rating score (0-5), 0 = not rated
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID, version),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE BookStatus (
    userID INT NOT NULL,
    bookID INT NOT NULL,
    version SERIAL NOT NULL,
    status VARCHAR(20) NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userID, bookID, version),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE Message (
    ID SERIAL NOT NULL,
    version SERIAL NOT NULL,
    userID INT NOT NULL,
    bookID INT NOT NULL,
    message TEXT NOT NULL,
    status VARCHAR(20) NOT NULL, -- (created, updated, deleted)
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID, version),
    FOREIGN KEY (userID) REFERENCES "User"(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE
);

-- Relationship Mapping Tables
CREATE TABLE BookGenre (
    bookID INT NOT NULL,
    genreID INT NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, genreID),
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (genreID) REFERENCES Genre(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE BookTag (
    bookID INT NOT NULL,
    tagID INT NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, tagID),
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (tagID) REFERENCES Tag(ID) ON
    DELETE
        CASCADE
);

CREATE TABLE BookType (
    bookID INT NOT NULL,
    typeID INT NOT NULL,
    createts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bookID, typeID),
    FOREIGN KEY (bookID) REFERENCES Book(ID) ON
    DELETE
        CASCADE,
        FOREIGN KEY (typeID) REFERENCES TYPE(ID) ON
    DELETE
        CASCADE
);


-- User activity indexes
CREATE INDEX score_user_btree ON Score(userID);

CREATE INDEX score_book_btree ON Score(bookID);

CREATE INDEX bookstatus_user_btree ON BookStatus(userID);

-- Book relationship indexes
CREATE INDEX message_user_btree ON Message(userID);

CREATE INDEX message_book_btree ON Message(bookID);

CREATE INDEX bookgenre_book_btree ON BookGenre(bookID);

CREATE INDEX booktag_book_btree ON BookTag(bookID);

CREATE INDEX booktype_book_btree ON BookType(bookID);
