---------------------------------------------------------
--  Up
---------------------------------------------------------
CREATE TABLE pinTracker (
  id          INT PRIMARY KEY,
  fileID      TEXT NOT NULL UNIQUE,
  ipfsAddress TEXT NOT NULL,
  pinCount    INT,
  lastCheck   INT,
  isPinned    BOOLEAN,
  pinToDo     BOOLEAN,
  bytes       INT,
  CONSTRAINT Post_ck_isPinned CHECK (isPinned IN (0, 1)),
  CONSTRAINT Post_ck_pinToDo CHECK (pinToDo IN (0, 1))
);

CREATE TABLE pinbotNodes (
  id          INT PRIMARY KEY,
  floAddress  TEXT,
  btcAddress  TEXT,
  ipnsAddress TEXT,
  ipfsAddress TEXT,
  active      BOOLEAN,
  CONSTRAINT Post_ck_active CHECK (active IN (0, 1))
);



---------------------------------------------------------
-- Down
---------------------------------------------------------

DROP TABLE pinbotNodes;
DROP TABLE pinTracker;