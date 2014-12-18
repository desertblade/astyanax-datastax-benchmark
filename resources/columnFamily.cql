CREATE TABLE users (
  userid text PRIMARY KEY,
  firstname text,
  lastname text,
);

INSERT INTO users (userid, firstname, lastname ) VALUES ( '1', 'test', 'test');
INSERT INTO users (userid, firstname, lastname ) VALUES ( '2', 'safsa', 'safzxvc');
INSERT INTO users (userid, firstname, lastname ) VALUES ( 'me', 'safsa', 'safzxvc');


CREATE TABLE users2 (
  userid text PRIMARY KEY,
  firstname text,
  lastname text,
) WITH COMPACT STORAGE;

INSERT INTO users2 (userid, firstname, lastname ) VALUES ( '1', 'test', 'test');
INSERT INTO users2 (userid, firstname, lastname ) VALUES ( '2', 'safsa', 'safzxvc');
INSERT INTO users2 (userid, firstname, lastname ) VALUES ( 'me', 'safsa', 'safzxvc');


