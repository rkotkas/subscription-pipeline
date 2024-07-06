import unittest
import sqlalchemy
import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from unittest.mock import patch, MagicMock



# Add the dev directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev')))

from pipeline_new import create_db_engine, get_table_names, get_dataframes, \
						process_dataframes, update_db_table, merge_df, check_for_updates


class TestPipelineFunctions(unittest.TestCase):

	def setUp(self):
		self.db_url = "sqlite:///../dev/cademycode.db"
		self.engine = create_db_engine(self.db_url)
		with self.engine.connect() as connection:
			connection.execute(text("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)"))
			connection.execute(text("INSERT INTO test_table (name) VALUES ('test_name')"))
		self.last_row_counts = {
			'cademycode_courses': 10,
			'cademycode_jobs': 20,
			'cademycode_students': 30
		}

	def tearDown(self):
		with self.engine.connect() as connection:
			connection.execute(text("DROP TABLE IF EXISTS test_table"))
		self.engine.dispose()

	def test_create_db_engine(self):
		with self.engine.connect() as connection:
			query = text("SELECT 1")
			result = connection.execute(query).fetchone()
			#print(result)
			#print("Connection successful!")
			assert result == (1, )


	@patch('pipeline.get_row_counts')
	def test_check_for_updates_no_change(self, mock_get_row_counts):
		# Mock the get_row_counts function to return the same counts
		mock_get_row_counts.return_value = {
			'cademycode_courses': 10,
			'cademycode_student_jobs': 20,
			'cademycode_students': 30
		}

		# Call the function and check the result
		has_updates = check_for_updates(self.engine, self.last_row_counts)
		self.assertFalse(has_updates)

	@patch('pipeline.get_row_counts')
	def test_check_for_updates_with_change(self, mock_get_row_counts):
		# Mock the get_row_counts function to return different counts
		mock_get_row_counts.return_value = {
			'cademycode_courses': 11,  # Change in the row count
			'cademycode_student_jobs': 20,
			'cademycode_students': 30
		}

		# Call the function and check the result
		has_updates = check_for_updates(self.engine, self.last_row_counts)
		self.assertTrue(has_updates)


	@patch('pipeline.get_row_counts')
	def test_check_for_updates_new_table(self, mock_get_row_counts):
		# Mock the get_row_counts function to include a new table
		mock_get_row_counts.return_value = {
			'cademycode_courses': 10,
			'cademycode_jobs': 20,
			'cademycode_students': 30,
			'new_table': 5  # New table added
		}

		# Call the function and check the result
		has_updates = check_for_updates(self.engine, self.last_row_counts)
		self.assertTrue(has_updates)


	def test_get_table_names(self):
		with self.engine.connect() as connection:
			connection.execute(text("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)"))
			connection.execute(text("INSERT INTO test_table (name) VALUES ('test_name')"))
		table_names = get_table_names(self.engine)
		#print(table_names)
		self.assertIn('test_table', table_names)


	def test_get_dataframes(self):
		dataframes = get_dataframes(self.engine)
		#print("DataFrames loaded from the database:")	
		for table_name, df in dataframes.items():
			#print(f"Table: {table_name}")
			#print(df.head())
			self.assertIsInstance(df, pd.DataFrame, f"{table_name} should be a DataFrame")
		#	self.assertFalse(df.empty, f"{table_name} should not be empty")


	def test_df_students_not_empty(self):
		dataframes = get_dataframes(self.engine)
		df_students = dataframes.get('cademycode_students', pd.DataFrame())
		#print("dataframe df_students:  ",df_students.head())
		self.assertFalse(df_students.empty, f"{df_students} should not be empty")

	def test_df_students_nan_dropped(self):
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		nan_counts = df_students.isna().sum()
		#print("nan dropped count: ", nan_counts)
		assert (nan_counts == 0).all()

	def test_df_students_dtypes(self):
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		#print("df_students dtype is: ", df_students['job_id'].dtype)
		# Check data types of columns
		assert df_students['job_id'].dtype in ['int32', 'int64'], "Column 'job_id' should have int64 data type"
		assert df_students['num_course_taken'].dtype in ['int32', 'int64'], "Column 'num_course_taken' should have int64 data type"
		assert df_students['time_spent_hrs'].dtype == 'float64', "Column 'time_spent_hrs' should have float64 data type"
		assert df_students['contact_info'].dtype == 'object', "Column 'contact_info' should have object (string) data type"
		assert df_students['current_career_path_id'].dtype in ['int32', 'int64'], "Column 'current_career_path_id' should have int64 data type"

	def test_df_jobs_not_empty(self):
		dataframes = get_dataframes(self.engine)
		df_jobs = dataframes.get('cademycode_student_jobs', pd.DataFrame())
		#print("dataframe df_students:  ",df_students.head())
		self.assertFalse(df_jobs.empty, f"{df_jobs} should not be empty")

	def test_df_jobs_duplicated_dropped(self):
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		duplicates = df_jobs.duplicated()
		#print(duplicates)
		assert not duplicates.any(), "There are duplicate rows in the DataFrame"


	def test_merged_df_row_count(self):
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		merged_df = merge_df(df_students, df_jobs, df_courses)
		expected_row_count = 5254
		self.assertEqual(len(merged_df), expected_row_count)


	def test_merged_df_columns(self):     
		# Check columns
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		merged_df = merge_df(df_students, df_jobs, df_courses)

		expected_columns = ['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id',
		'num_course_taken', 'current_career_path_id', 'time_spent_hrs',
		'job_category', 'avg_salary', 'career_path_id', 'career_path_name',
		'hours_to_complete']

		#print("expected: ", expected_columns)
		self.assertListEqual(list(merged_df.columns), expected_columns)
        

if __name__ == '__main__':
    unittest.main()

'''
	def test_update_db_table_dtypes(self):
		dataframes = get_dataframes(self.engine)
		df_students, df_jobs, df_courses = process_dataframes(dataframes)
		df_students_head = df_students.head()
		try:
			update_db_table(self.engine, df_students_head)
		except Exception as e:
			self.fail(f"update_db_table raised an exception {e}")

		# Verify the table was updated correctly
		with self.engine.connect() as connection:
			result = connection.execute(text("SELECT * FROM cademycode_students"))
			rows = result.fetchall()
			self.assertEqual(len(rows), len(df_students))
'''