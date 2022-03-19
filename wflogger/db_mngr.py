import os
import psycopg2

from credentials import creds


CREATE_TABLE_SQL = """CREATE TABLE workflow_logs (
  id            serial PRIMARY KEY,
  user_id       varchar(32) NOT NULL,
  hostname      varchar(64) NOT NULL,
  workflow      varchar(64) NOT NULL,
  tag           varchar(64) NOT NULL,
  stage_number  integer NOT NULL,
  stage         varchar(64) NOT NULL,
  iteration     integer DEFAULT 0,
  date_time     timestamp DEFAULT current_timestamp,
  comment       varchar(128) DEFAULT '',
  flag          integer DEFAULT -999
);"""

DROP_TABLE_SQL = "DROP TABLE workflow_logs;"


def create_db():
    with psycopg2.connect(creds) as conn:
        with conn.cursor() as curs:
            curs.execute(CREATE_TABLE_SQL)

def drop_db():
    with psycopg2.connect(creds) as conn:
        with conn.cursor() as curs:
            curs.execute(DROP_TABLE_SQL)


if __name__ == "__main__":
 #   drop_db()
    create_db()
