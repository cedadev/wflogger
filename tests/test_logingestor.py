import unittest
import tempfile
import os
import logging

"""Some basic unit tests for the LogIngestor module"""

from wflogger.log_ingestor import LogIngestor

# test that should ingest correctly
loglines1 = """
Lorem Ipsum
Lorem Ipsum WFL_START fred | compute1 | modeler.py | v14.3 | 1 | prep | 0 | 2022-01-01 12:23:04.342912 ||
Lorem Ipsum WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 1 | 2022-01-01 12:23:05.927111 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 2 | 2022-01-01 12:25:02.891922 ||
Lorem Ipsum WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 3 | 2022-01-01 12:28:05.782933 ||
||| WFL_START fred | compute1 | modeler.py | v14.3 | 3 | publish | 0 | 2022-01-01 12:31:01.124212 ||
"""

# test with a problem in the timestamp field
loglines_load_error2 = """
WFL_START fred | compute1 | modeler.py | v14.3 | 1 | prep | 0 | 2022-01-01 12:23:04.342912 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 1 | 2022-01-01 12:23:05.927111 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 2 | 202-01-01 12:25:02.891922 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 3 | 2022-01-01 12:28:05.782933 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 3 | publish | 0 | 2022-01-01 12:31:01.124212 ||
"""

# test that should ingest correctly
loglines3 = """
Lorem Ipsum WFL_START fred | compute1 | modeler.py | v14.3 | 1 |prep | 0 | 2022-01-01 12:23:04.342912 |Lorem Ipsum|
Lorem Ipsum WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 1 | 2022-01-01 12:23:05.927111 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 2| 2022-01-01 12:25:02.891922 ||
Lorem Ipsum
Lorem Ipsum WFL_START fred | compute1 | modeler.py |v14.3 |2 | model | 3 | 2022-01-01 12:28:05.782933 ||
Lorem Ipsum
||| WFL_START fred | compute1 | modeler.py | v14.3 | 3 | publish | 0 | 2022-01-01 12:31:01.124212 ||
||| WFL_START fred | compute1 | modeler.py | v14.3 | 3 | publish | 1 | 2022-01-01 12:35:33.224452 ||11
"""

# test with a problem in the stage_number field
loglines_load_error4 = """
WFL_START fred | compute1 | modeler.py | v14.3 | 1 | prep | 0 | 2022-01-01 12:23:04.342912 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 1 | 2022-01-01 12:23:05.927111 ||
WFL_START fred | compute1 | modeler.py | v14.3 | s2 | model | 2 | 202-01-01 12:25:02.891922 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 2 | model | 3 | 2022-01-01 12:28:05.782933 ||
WFL_START fred | compute1 | modeler.py | v14.3 | 3 | publish | 0 | 2022-01-01 12:31:01.124212 ||
"""

# TODO add some more detailed checks on the ingested data to ensure it matches the expected values

class LogIngestorTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.CRITICAL)
        self.db_path = tempfile.mktemp(suffix=".db")

    def tearDown(self):
        os.remove(self.db_path)
        self.db_path = None

    def test_load1(self):
        try:
            log_path = tempfile.mktemp(suffix=".log")
            with open(log_path, "w") as f:
                f.write(loglines1)
            ls = LogIngestor(sqlite_database_path=self.db_path)
            ls.prepare_database()
            self.assertTrue(ls.ingest_log(log_path))
            self.assertEqual(ls.workflow_logs_table_size(),5)
            ls.reset_database()
            self.assertEqual(ls.workflow_logs_table_size(), 0)
        finally:
            os.remove(log_path)

    def test_load_error2(self):
        try:
            log_path = tempfile.mktemp(suffix=".log")
            with open(log_path, "w") as f:
                f.write(loglines_load_error2)
            ls = LogIngestor(sqlite_database_path=self.db_path)
            ls.prepare_database()
            self.assertFalse(ls.ingest_log(log_path))
            self.assertEqual(ls.workflow_logs_table_size(), 0)
        finally:
            os.remove(log_path)

    def test_load3(self):
        try:
            log_path = tempfile.mktemp(suffix=".log")
            with open(log_path, "w") as f:
                f.write(loglines3)
            ls = LogIngestor(sqlite_database_path=self.db_path)
            ls.prepare_database()
            self.assertTrue(ls.ingest_log(log_path))
            self.assertEqual(ls.workflow_logs_table_size(),6)
        finally:
            os.remove(log_path)

    def test_load_error4(self):
        try:
            log_path = tempfile.mktemp(suffix=".log")
            with open(log_path, "w") as f:
                f.write(loglines_load_error4)
            ls = LogIngestor(sqlite_database_path=self.db_path)
            ls.prepare_database()
            self.assertFalse(ls.ingest_log(log_path))
            self.assertEqual(ls.workflow_logs_table_size(), 0)
        finally:
            os.remove(log_path)

if __name__ == '__main__':
    unittest.main()
