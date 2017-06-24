---------------------------------------------------------
--  Up
---------------------------------------------------------

ALTER TABLE pinTracker ADD COLUMN fileAddress TEXT;

---------------------------------------------------------
-- Down
---------------------------------------------------------

PRAGMA foreign_keys=off;

ALTER TABLE pinTracker RENAME TO _pinTracker_old;

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

INSERT INTO pinTracker (id, fileID, ipfsAddress, pinCount, lastCheck, isPinned, pinToDo, bytes)
  SELECT id, fileID, ipfsAddress, pinCount, lastCheck, isPinned, pinToDo, bytes
  FROM _pinTracker_old;

DROP TABLE _pinTracker_old;

PRAGMA foreign_keys=on;