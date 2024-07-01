import unittest
from your_script_name import create_db_connection, check_for_updates, update_database, process_dataframes, save_dataframes
import sqlalchemy

class TestDatabaseFunctions(unittest.TestCase):
    
    def setUp(self):
        self.engine = create_db_connection("sqlite:///test_cademycode.db")
        # Initialize test database state

    def test_create_db_connection(self):
        self.assertIsInstance(self.engine, sqlalchemy.engine.base.Engine)

    def test_check_for_updates(self):
        last_row_counts = {"test_table": 10}
        self.assertFalse(check_for_updates(self.engine, last_row_counts))

    def test_update_database(self):
        dataframes = update_database(self.engine)
        self.assertIsInstance(dataframes, dict)

    def test_process_dataframes(self):
        dataframes = update_database(self.engine)
        df_dropped = process_dataframes(dataframes)
        self.assertNotIn('NaN', df_dropped.values)

    def test_save_dataframes(self):
        dataframes = update_database(self.engine)
        df_dropped = process_dataframes(dataframes)
        try:
            save_dataframes(self.engine, df_dropped)
        except Exception as e:
            self.fail(f"save_dataframes raised an exception {e}")

if __name__ == '__main__':
    unittest.main()
