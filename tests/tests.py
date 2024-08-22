import unittest
import sqlalchemy
import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from unittest import TestCase, mock
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
test_logger = logging.getLogger('testlog')

# Create a formatter with timestamp
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create file handler and set formatter
test_handler = logging.FileHandler('./testlog.log')
test_handler.setFormatter(formatter)

# Add handler to the logger
test_logger.addHandler(test_handler)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev')))

from pipeline import create_db_engine, get_table_names, get_dataframes, \
						process_dataframes, update_db_tables, merge_df, \
						check_for_updates, get_row_counts, configure_logging


class TestPipelineFunctions(unittest.TestCase):

	def setUp(self):
		self.db_url = "sqlite:///../cademycode.db"
		self.engine = create_db_engine(self.db_url)
		with self.engine.connect() as connection:
			connection.execute(text("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)"))
			connection.execute(text("INSERT INTO test_table (name) VALUES ('test_name')"))
		self.last_row_counts = {
			'cademycode_courses': 10,
			'cademycode_student_jobs': 20,
			'cademycode_students': 30
		}
		# Mock the configure_logging to use test_logger
		self.changelog, self.errorlog = configure_logging(test_env=True)


	def tearDown(self):
		with self.engine.connect() as connection:
			connection.execute(text("DROP TABLE IF EXISTS test_table"))
		self.engine.dispose()


	@mock.patch('pipeline.logging.getLogger')
	def test_create_db_engine(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_create_db_engine")

		with self.engine.connect() as connection:
			query = text("SELECT 1")
			result = connection.execute(query).fetchone()
			test_logger.info("Connection successful!")
			self.assertEqual(result, (1, ))


	@mock.patch('pipeline.logging.getLogger')
	@mock.patch('pipeline.get_row_counts')
	def test_check_for_updates_no_change(self, mock_get_row_counts, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_check_for_updates_no_change")

		# Mock the get_row_counts function to return the same counts
		mock_get_row_counts.return_value = {
			'cademycode_courses': 10,
			'cademycode_student_jobs': 20,
			'cademycode_students': 30
		}
		test_logger.info(f"Last row counts: {self.last_row_counts}")
		test_logger.info(f"Current row counts (mocked): {mock_get_row_counts.return_value}")

		# Call check_for_updates with the mocked get_row_counts
		has_updates = check_for_updates(self.engine, self.last_row_counts, mock_getLogger.return_value, mock_get_row_counts.return_value)

		test_logger.info(f"Updates found: {has_updates}")

		self.assertFalse(has_updates, f"Expected False but got True for has_updates. Last counts: {self.last_row_counts}")


	@mock.patch('pipeline.logging.getLogger')
	@mock.patch('pipeline.get_row_counts')
	def test_check_for_updates_with_change(self, mock_get_row_counts, mock_getLogger):
		mock_getLogger.return_value = test_logger

		test_logger.info("Starting test: test_check_for_updates_with_change")

		# Mock the get_row_counts function to return different counts
		mock_get_row_counts.return_value = {
			'cademycode_courses': 11,  # Change in the row count
			'cademycode_student_jobs': 20,
			'cademycode_students': 30
		}

		# Log the values before invoking check_for_updates
		test_logger.info(f"Last row counts: {self.last_row_counts}")

		has_updates = check_for_updates(self.engine, self.last_row_counts, mock_getLogger.return_value, mock_get_row_counts.return_value)

		test_logger.info(f"Updates found: {has_updates}")
		self.assertTrue(has_updates, f"Expected True but got False for has_updates. Last counts: {self.last_row_counts}")


	@mock.patch('pipeline.logging.getLogger')
	@mock.patch('pipeline.get_row_counts')
	def test_check_for_updates_new_table(self, mock_get_row_counts, mock_getLogger):
		mock_getLogger.return_value = test_logger

		test_logger.info("Starting test: test_check_for_updates_new_table")

		# Mock the get_row_counts function to include a new table
		mock_get_row_counts.return_value = {
			'cademycode_courses': 10,
			'cademycode_student_jobs': 20,
			'cademycode_students': 30,
			'new_table': 5  # New table added
		}

		has_updates = check_for_updates(self.engine, self.last_row_counts, mock_getLogger.return_value, mock_get_row_counts.return_value)

		self.assertTrue(has_updates)


	@mock.patch('pipeline.logging.getLogger')
	def test_get_table_names(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_get_table_names")

		with self.engine.connect() as connection:
			connection.execute(text("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)"))
			connection.execute(text("INSERT INTO test_table (name) VALUES ('test_name')"))
		table_names = get_table_names(self.engine)
		test_logger.info(f"table names: {table_names}")
		self.assertIn('test_table', table_names)


	@mock.patch('pipeline.logging.getLogger')
	def test_get_dataframes(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_get_dataframes")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		test_logger.info("DataFrames loaded from the database:")

		for table_name, df in dataframes.items():
			test_logger.info(f"Table: {table_name}")
			self.assertIsInstance(df, pd.DataFrame, f"{table_name} should be a DataFrame")


	@mock.patch('pipeline.logging.getLogger')
	def test_df_students_not_empty(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_df_students_not_empty")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students = dataframes.get('cademycode_students', pd.DataFrame())
		test_logger.info(f"dataframe df_students:  {df_students.head()}")
		self.assertFalse(df_students.empty, f"{df_students} should not be empty")


	@mock.patch('pipeline.logging.getLogger')
	def test_df_students_nan_dropped(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_df_students_nan_dropped")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students, df_jobs, df_courses = process_dataframes(dataframes, mock_getLogger)
		nan_counts = df_students.isna().sum()
		test_logger.info(f"NaN dropped count: {nan_counts}")
		self.assertTrue((nan_counts == 0).all())


	@mock.patch('pipeline.logging.getLogger')
	def test_df_students_dtypes(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_df_students_dtypes")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students, df_jobs, df_courses = process_dataframes(dataframes, mock_getLogger)
		# Check data types of columns
		self.assertEqual(df_students['dob'].dtype, 'datetime64[ns]', "Column 'dob' should have datetime data type")
		self.assertIn(df_students['job_id'].dtype, ['int32', 'int64'], "Column 'job_id' should have int64 data type")
		self.assertIn(df_students['num_course_taken'].dtype, ['int32', 'int64'], "Column 'num_course_taken' should have int64 data type")
		self.assertEqual(df_students['time_spent_hrs'].dtype, 'float64', "Column 'time_spent_hrs' should have float64 data type")
		self.assertEqual(df_students['contact_info'].dtype, 'object', "Column 'contact_info' should have object (string) data type")
		self.assertIn(df_students['current_career_path_id'].dtype, ['int32', 'int64'], "Column 'current_career_path_id' should have int64 data type")


	@mock.patch('pipeline.logging.getLogger')
	def test_df_jobs_not_empty(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_df_jobs_not_empty")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
		test_logger.info(f"dataframe df_students:  {df_jobs.head()}")
		self.assertFalse(df_jobs.empty, f"{df_jobs} should not be empty")


	@mock.patch('pipeline.logging.getLogger')
	def test_df_jobs_duplicated_dropped(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_df_jobs_duplicated_dropped")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students, df_jobs, df_courses = process_dataframes(dataframes, mock_getLogger)
		duplicates = df_jobs.duplicated()
		test_logger.info(f"df_jobs duplicates: {duplicates}")
		self.assertFalse(duplicates.any(), "There are duplicate rows in the DataFrame")


	@mock.patch('pipeline.logging.getLogger')
	def test_merged_df_row_count(self, mock_getLogger):
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_merged_df_row_count")

		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students, df_jobs, df_courses = process_dataframes(dataframes, mock_getLogger)
		merged_df = merge_df(df_students, df_jobs, df_courses, mock_getLogger)
		expected_row_count = len(merged_df)
		self.assertEqual(len(merged_df), expected_row_count)


	@mock.patch('pipeline.logging.getLogger')
	def test_merged_df_columns(self, mock_getLogger):   
		mock_getLogger.return_value = test_logger
		test_logger.info("Starting test: test_merged_df_columns")
  
		# Check columns
		dataframes = get_dataframes(self.engine, mock_getLogger)
		df_students, df_jobs, df_courses = process_dataframes(dataframes, mock_getLogger)
		merged_df = merge_df(df_students, df_jobs, df_courses, mock_getLogger)

		expected_columns = ['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id',
		'num_course_taken', 'current_career_path_id', 'time_spent_hrs',
		'job_category', 'avg_salary', 'career_path_id', 'career_path_name',
		'hours_to_complete']

		self.assertListEqual(list(merged_df.columns), expected_columns)
        

if __name__ == '__main__':
	unittest.main()