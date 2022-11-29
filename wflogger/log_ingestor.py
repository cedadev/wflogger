"""
A python3 script for ingesting workflow logging information from log files into a database
Supports either the standard workflow logger postgres database (default) or alternatively a sqlite3 database (useful for testing).

Allows data to be gathered from an application log where real time collection is not possible

Some example usages:

(1) ingest from log file /tmp/log1.txt

python log_ingestor.py /tmp/log1.txt

(2) ingest from log files from /tmp/test1/a/*.log and /tmp/test1/b/*.log, writing verbose output

python log_ingestor.py --verbose /tmp/test1/a/*.log /tmp/test1/b/*.log

(3) clear the database and ingest from log files from /tmp/test1/a/*.log

python log_ingestor.py --reset /tmp/test1/a/*.log

(4) using a sqlite3 database rather than postgres, setup the database and ingest from log files from /tmp/test1/b/*.log

python log_ingestor.py --sqlite-path test.db --setup /tmp/test1/b/*.log

Log line format:

Log lines that contain the token WFL_START will be interepreted as | delimited workflow log entries after the token:

... WFL_ENTRY <user_id> | <hostname> | <workflow> | <tag> | <stage_number> | <stage> | <iteration> | <date_time> | <flag> | <comment>

where:
    <user_id> is a string username (32 chars max)
    <hostname> is a string hostname (64 chars max)
    <workflow> identifies the type of job (64 chars max)
    <tag> provides additional information on the job (64 chars max)
    <stage_number> is an integer identifying the processing stage within the job
    <stage> is a string name describing the processing stage
    <iteration> is the integer iteration number within the stage (may be blank)
    <date_time> is the date-time including microseconds, formatted exactly thus: 2022-11-28 14:34:27.393315
    <comment> is an additional comment (may be blank)
    <flag> is an additional integer tag (may be blank)

Note:
    fields should not contain the | symbol
"""

import logging
import datetime
import os.path

INSERT_SQL = """INSERT INTO workflow_logs
  (user_id, hostname, workflow, tag, stage_number, stage,
  iteration, date_time, comment, flag)
  VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

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

DELETE_FROM_TABLE_SQL = "DELETE FROM workflow_logs;"

COUNT_QUERY_SQL = "SELECT COUNT(*) FROM workflow_logs;"


class ParsingError(ValueError):
    """Exception sub-class to describe an error encountered attempting to parse a log line"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LogIngestor:
    """Utility class to scan log files, extract embedded WORKFLOW LOG entries, and populate the database"""

    START_TOKEN = "WFL_START"

    DEFAULT_ITERATION = 0
    DEFAULT_FLAG = -999

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, sqlite_database_path=None, verbose=False):
        """
        Constructor

        :param sqlite_database_path: write to an SQL database at this path instead of Postgres
        """
        self.logger = logging.getLogger("LogScanner")
        self.insert_sql = INSERT_SQL
        if sqlite_database_path:
            import sqlite3
            self.conn = sqlite3.connect(sqlite_database_path)
            # Sqlite uses ? rather than %s paramstyle, customise the insert SQL accordingly
            self.insert_sql = self.insert_sql.replace("%s", "?")
            self.logger.info("Writing to Sqlite database file %s" % sqlite_database_path)
        else:
            import psycopg2
            creds_file = os.path.join(os.environ.get("HOME"), ".wflogger")
            if not os.path.isfile(creds_file):
                raise IOError(f"Required credentials file does not exist: {creds_file}")
            status = os.stat(creds_file)
            if oct(status.st_mode)[-3:] != "400":
                raise PermissionError(f"File permissions on credentials file must be read-only for user: 0400")
            creds = open(creds_file).read()
            self.conn = psycopg2.connect(creds)
            self.logger.info("Writing to postgres database using credentials in file %s" % creds_file)

        # track some stats
        self.ingested_files = 0
        self.failed_files = 0
        self.entries_ingested = 0

    def stats(self):
        """
        Return stats on the processing completed by this instance

        :return: (nr-files-ingested-successfully,nr-files-failed-to-ingest,total-entries-ingested)
        """
        return (self.ingested_files,self.failed_files,self.entries_ingested)

    def prepare_database(self):
        """
        Attempt to prepare the database by creating the workflow_logs table

        Will log and then ignore any errors (for example, table already exists)
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(CREATE_TABLE_SQL)
        except Exception as ex:
            self.logger.exception(ex)

    def reset_database(self):
        """
        Attempt to remove all rows from the workflow_logs table

        Will log and then ignore any errors (for example, table does not exist)
        """
        try:
            curs = self.conn.cursor()
            curs.execute(DELETE_FROM_TABLE_SQL)
        except Exception as ex:
            self.logger.exception(ex)

    def workflow_logs_table_size(self):
        """
        Get the number of rows in the workflow_logs table

        :return: number of rows or -1 if error encountered
        """
        try:
            cursor = self.conn.cursor()
            rs = cursor.execute(COUNT_QUERY_SQL).fetchall()
            return rs[0][0]
        except Exception as ex:
            self.logger.exception(ex)
            return -1

    def ingest_log(self, path):
        """
        Ingest entries from a log file into the database

        :param path: the filesystem path of the log file
        :return: True iff at least one entry was found and ALL found entries were successfully ingested
        """
        with open(path) as f:
            try:
                # collect all the entries in the log file
                entries = []
                line_nr = 0
                for logline in f.readlines():
                    line_nr += 1
                    if LogIngestor.START_TOKEN in logline:
                        start_index = logline.find(LogIngestor.START_TOKEN)
                        entry = self.__parse_entry(logline[start_index + len(LogIngestor.START_TOKEN):], line_nr)
                        entries.append(entry)
                # bulk insert the entries
                nr_entries = len(entries)
                if nr_entries > 0:
                    cursor = self.conn.cursor()
                    cursor.executemany(self.insert_sql, entries)
                    self.conn.commit()
                    self.logger.info("Ingested %d entries from log file %s" % (nr_entries, path))
                    self.ingested_files += 1
                    self.entries_ingested += nr_entries
                    return True
                else:
                    # no entries found, treat as a fail
                    self.failed_files += 1
                    return False
            except ParsingError as ex:
                # if there are problems parsing this log file - rollback any updates
                self.conn.rollback()
                self.logger.error("Unable to ingest log file %s due to error: %s" % (path, str(ex)))
                return False

    def __parse_entry(self, entry, line_nr):
        """
        Parse an entry from a log line.  The entry is the remainder of the line after the marker WFL_START

        :param entry: string containing the log line after the WFL_START token
        :param line_nr: the line number of the log line
        :return: 10-tuple containing the parsed entry

        :raises ParserError if there was a problem reading the entry
        """
        components = list(map(lambda s: s.strip(), entry.split("|")))
        if len(components) != 10:
            raise ParsingError("At line %d: line does not contain the required 10 |-delimited fields, found %d fields"
                               % (line_nr, len(components)))
        user_id = components[0]
        hostname = components[1]
        workflow = components[2]
        tag = components[3]
        stage_number = self.__parse_integer(components[4], "stage_number", line_nr)
        stage = components[5]
        iteration = self.__parse_integer(components[6], "iteration", line_nr) if components[6] else 0
        date_time = self.__parse_date(components[7], "date_time", line_nr)
        comment = components[8]
        flag = self.__parse_integer(components[9], "flag", line_nr) if components[9] else -999
        return (user_id, hostname, workflow, tag, stage_number, stage, iteration, date_time, comment, flag)

    def __parse_integer(self, s, field_name, line_nr):
        """
        Parse an integer from one of the string fields in an entry

        :param s: raw field string
        :param field_name: the name of the field being parsed
        :param line_nr: the line number of the log line being parsed
        :return: integer successfully parsed from the entry

        :raises ParserError if there was a problem reading the entry
        """
        try:
            return int(s)
        except ValueError:
            raise ParsingError("At line %d: Could not parse field %s value %s as integer"
                               % (line_nr, field_name, s))

    def __parse_date(self, s, field_name, line_nr):
        """
        Parse a datetime value from one of the string fields in an entry
        The entry must be of the format described by example 2022-11-28 14:34:27.393315

        :param s: raw field string
        :param field_name: the name of the field being parsed
        :param line_nr: the line number of the log line being parsed
        :return: python datetime.datetime object successfully parsed from the entry

        :raises ParserError if there was a problem reading the entry
        """
        try:
            return datetime.datetime.strptime(s, LogIngestor.DATETIME_FORMAT)
        except ValueError:
            raise ParsingError("At line %d: Could not parse field %s value %s as datetime with format %s"
                               % (line_nr, field_name, s, LogIngestor.DATETIME_FORMAT))

if __name__ == '__main__':
    import argparse
    import glob

    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", nargs="+", default=[], help="Specify one or more patterns to locate log files")

    parser.add_argument("--setup", action="store_true", help="Setup database")
    parser.add_argument("--reset", action="store_true", help="Reset database")
    parser.add_argument("--verbose", action="store_true", help="Log verbose messages to console")

    parser.add_argument("--sqlite-path", default=None,
                        help="Write to a SQLite database with the specified path - useful for debugging")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    ls = LogIngestor(args.sqlite_path)
    if args.setup:
        ls.prepare_database()
    if args.reset:
        ls.reset_database()
    for filepath in args.pattern:
        matching_paths = glob.glob(filepath)
        for matching_path in matching_paths:
            ls.ingest_log(matching_path)
    (ingested_files,failed_files,entries_ingested) = ls.stats()
    print("LogIngestor Summary: Ingested %d entries total from %d files, failed to ingest %d files" \
           % (entries_ingested, ingested_files, failed_files))
