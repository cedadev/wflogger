import datetime as dt
import psycopg2


from .credentials import creds, user_id, hostname


INSERT_SQL = """INSERT INTO workflow_logs
  (user_id, hostname, workflow, tag, stage_number, stage,
  iteration, date_time, comment, flag)
  VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

DEFAULT_ITERATION = 0
DEFAULT_FLAG = -999


def insert_record(workflow, tag, stage_number, stage, iteration=0,
                  date_time=None, comment="", flag=DEFAULT_FLAG):

    if date_time:
        date_time = parser.parse(date_time)
    else:
        date_time = dt.datetime.now()

    with psycopg2.connect(creds) as conn:
        with conn.cursor() as curs:
            curs.execute(INSERT_SQL,
                (user_id, hostname, workflow, tag, stage_number,
                 stage, iteration, date_time, comment, flag))


